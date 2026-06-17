"""
Telegram Bot - Transfert de médias entre canaux (y compris canaux restreints)
Utilise Telethon (session utilisateur) pour accéder aux canaux sans forwarding
et un client bot pour les commandes.
"""

import asyncio
import json
import logging
import os
from datetime import datetime
from io import BytesIO
from zoneinfo import ZoneInfo

PARIS = ZoneInfo("Europe/Paris")


def now_paris() -> datetime:
    return datetime.now(PARIS)

from telethon import TelegramClient, events
from telethon.errors import (
    FloodWaitError,
    MediaEmptyError,
    InviteHashInvalidError,
    UserAlreadyParticipantError,
)
from telethon.sessions import StringSession
from telethon.tl.functions.bots import SetBotCommandsRequest
from telethon.tl.functions.messages import (
    CheckChatInviteRequest,
    GetHistoryRequest,
    ImportChatInviteRequest,
)
from telethon.tl.types import (
    BotCommand,
    BotCommandScopeDefault,
    ChatInviteAlready,
    ChatInvitePeek,
    MessageMediaDocument,
    MessageMediaPhoto,
    PeerChannel,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("MediaBot")

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
SESSION_STRING = os.environ.get("SESSION_STRING", "")
BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID = int(os.environ["OWNER_ID"])

DATA_FILE = "bot_data.json"

UPLOAD_SEMAPHORE = asyncio.Semaphore(5)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(8)

# Déduplication en mémoire : évite le double-envoi si l'event se déclenche 2x
_seen_messages: set[tuple[int, int]] = set()

# Cache entité destination (évite get_entity() à chaque message)
_dest_entity_cache = None
_dest_entity_cache_id: int | None = None

# Buffer albums : grouped_id → [(message, source_name, chat_id)]
_album_buffer: dict[int, list] = {}
_album_flush_tasks: dict[int, asyncio.Task] = {}
ALBUM_WAIT = 0.8  # secondes pour collecter tous les médias d'un album

user_client: TelegramClient = None
bot_client: TelegramClient = None


class BotData:
    def __init__(self):
        self.source_channels: list[dict] = []
        self.destination: str | None = None
        self.destination_id: int | None = None
        self.paused: bool = False
        self.stats: dict = {"today": 0, "total": 0, "date": str(datetime.now().date())}
        self.history_ids: set[int] = set()
        self.invite_cache: dict[str, int] = {}  # invite_hash → channel_id
        self._load()

    def _load(self):
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, "r") as f:
                    raw = json.load(f)
                self.source_channels = raw.get("source_channels", [])
                self.destination = raw.get("destination")
                self.destination_id = raw.get("destination_id")
                self.paused = raw.get("paused", False)
                self.stats = raw.get("stats", self.stats)
                self.history_ids = set(raw.get("history_ids", []))
                self.invite_cache = raw.get("invite_cache", {})
                logger.info(
                    f"Données chargées: {len(self.source_channels)} canaux source, "
                    f"destination={self.destination}"
                )
            except Exception as e:
                logger.error(f"Erreur chargement données: {e}")

    def save(self):
        try:
            with open(DATA_FILE, "w") as f:
                json.dump(
                    {
                        "source_channels": self.source_channels,
                        "destination": self.destination,
                        "destination_id": self.destination_id,
                        "paused": self.paused,
                        "stats": self.stats,
                        "history_ids": list(self.history_ids),
                        "invite_cache": self.invite_cache,
                    },
                    f,
                    indent=2,
                )
        except Exception as e:
            logger.error(f"Erreur sauvegarde: {e}")

    def reset_stats_if_new_day(self):
        today = str(datetime.now().date())
        if self.stats.get("date") != today:
            self.stats["today"] = 0
            self.stats["date"] = today
            self.save()

    def increment_stats(self, count=1):
        self.reset_stats_if_new_day()
        self.stats["today"] = self.stats.get("today", 0) + count
        self.stats["total"] = self.stats.get("total", 0) + count
        self.save()


data = BotData()

# ── Helpers globaux ───────────────────────────────────────────────────────────

_EXT_MAP = {
    "video/mp4": "mp4", "video/quicktime": "mov",
    "video/x-matroska": "mkv", "video/webm": "webm",
    "video/avi": "avi", "video/3gpp": "3gp",
    "image/jpeg": "jpg", "image/png": "png",
    "image/gif": "gif", "image/webp": "webp",
}


def _media_to_file(message, media_bytes: bytes):
    """Prépare (file_obj, extra_kwargs) pour send_file selon le type de média.
    Retourne (None, None) si type non supporté.
    """
    if isinstance(message.media, MessageMediaPhoto):
        return media_bytes, {"force_document": False}

    if isinstance(message.media, MessageMediaDocument):
        doc = message.media.document
        mime = doc.mime_type or ""
        attrs = doc.attributes or []
        is_video = mime.startswith("video/") or any(
            type(a).__name__ == "DocumentAttributeVideo" for a in attrs
        )
        is_gif = mime == "image/gif" or any(
            type(a).__name__ == "DocumentAttributeAnimated" for a in attrs
        )
        is_image = mime.startswith("image/") and not is_gif

        if is_video:
            ext = _EXT_MAP.get(mime, "mp4")
            bio = BytesIO(media_bytes)
            bio.name = f"video.{ext}"
            return bio, {"force_document": False, "supports_streaming": True}
        elif is_gif:
            bio = BytesIO(media_bytes)
            bio.name = "animation.gif"
            return bio, {"force_document": False}
        elif is_image:
            ext = _EXT_MAP.get(mime, "jpg")
            bio = BytesIO(media_bytes)
            bio.name = f"photo.{ext}"
            return bio, {"force_document": False}
        else:
            raw_ext = mime.split("/")[-1] if "/" in mime else "bin"
            bio = BytesIO(media_bytes)
            bio.name = f"file.{raw_ext}"
            return bio, {"force_document": True}

    return None, None


async def get_dest_entity():
    """Cache l'entité destination — évite get_entity() à chaque message."""
    global _dest_entity_cache, _dest_entity_cache_id
    if _dest_entity_cache is not None and _dest_entity_cache_id == data.destination_id:
        return _dest_entity_cache
    if not data.destination_id:
        return None
    try:
        _dest_entity_cache = await user_client.get_entity(PeerChannel(data.destination_id))
        _dest_entity_cache_id = data.destination_id
        return _dest_entity_cache
    except Exception as e:
        logger.error(f"Impossible de récupérer la destination (id={data.destination_id}): {e}")
        return None


def invalidate_dest_cache():
    global _dest_entity_cache, _dest_entity_cache_id
    _dest_entity_cache = None
    _dest_entity_cache_id = None


async def resolve_channel(identifier: str):
    """
    Résout un canal depuis un lien public, username ou lien d'invitation privé.
    Pour les liens privés (+HASH), évite CheckChatInviteRequest (FloodWait fréquent)
    en cherchant d'abord dans les canaux déjà connus, puis via ImportChatInviteRequest.
    """
    identifier = identifier.strip()

    # Extraire la partie utile des URLs t.me
    if "t.me/" in identifier:
        identifier = identifier.split("t.me/")[-1].strip("/")

    # ── Lien d'invitation privé (+HASH) ──────────────────────────────────────
    if identifier.startswith("+"):
        invite_hash = identifier[1:]

        def _extract_hash(link: str) -> str:
            if "+" in link:
                return link.split("+")[-1].strip("/")
            return ""

        # 1) Cache invite_hash → channel_id (aucun appel réseau)
        if invite_hash in data.invite_cache:
            try:
                return await user_client.get_entity(PeerChannel(data.invite_cache[invite_hash]))
            except Exception:
                del data.invite_cache[invite_hash]
                data.save()

        # 2) Canaux connus (source + destination) → lookup par ID stocké
        known_links = [(ch.get("link", ""), ch["id"]) for ch in data.source_channels]
        if data.destination and data.destination_id:
            known_links.append((data.destination, data.destination_id))

        for link, cid in known_links:
            if _extract_hash(link) == invite_hash:
                try:
                    entity = await user_client.get_entity(PeerChannel(cid))
                    data.invite_cache[invite_hash] = cid
                    data.save()
                    return entity
                except Exception:
                    pass

        # 3) ImportChatInviteRequest : rejoint le canal si pas encore membre
        try:
            joined = await user_client(ImportChatInviteRequest(hash=invite_hash))
            entity = joined.chats[0]
            data.invite_cache[invite_hash] = entity.id
            data.save()
            return entity
        except UserAlreadyParticipantError:
            # Déjà membre → CheckChatInviteRequest retourne directement l'entité du canal
            try:
                result = await user_client(CheckChatInviteRequest(hash=invite_hash))
                if isinstance(result, (ChatInviteAlready, ChatInvitePeek)):
                    entity = result.chat
                    data.invite_cache[invite_hash] = entity.id
                    data.save()
                    return entity
            except FloodWaitError as fw:
                mins = fw.seconds // 60 + 1
                secs = fw.seconds % 60
                raise ValueError(
                    f"⏳ Tu es déjà dans ce canal, mais Telegram bloque les vérifications "
                    f"pendant encore **{mins}min {secs}s**.\n"
                    f"Réessaie dans {mins} minutes."
                )
            except Exception:
                pass
            raise ValueError(
                f"⚠️ Tu es déjà dans ce canal mais il est introuvable.\n"
                f"Réessaie dans quelques minutes."
            )
        except InviteHashInvalidError:
            raise ValueError(f"Lien d'invitation invalide : `+{invite_hash}`")
        except FloodWaitError as e:
            raise ValueError(
                f"⏳ Telegram demande d'attendre {e.seconds}s "
                f"({e.seconds // 60 + 1} min). Réessaie après."
            )
        except Exception as e:
            raise ValueError(f"Impossible de rejoindre le canal `+{invite_hash}`: {e}")

    # ── Canal public (@username ou ID) ───────────────────────────────────────
    if not identifier.startswith("@"):
        identifier = "@" + identifier

    try:
        return await user_client.get_entity(identifier)
    except Exception:
        try:
            return await user_client.get_entity(identifier.lstrip("@"))
        except Exception as e:
            raise ValueError(f"Impossible de résoudre le canal `{identifier}`: {e}")


async def send_media_to_destination(message, caption_override: str = None) -> bool:
    """Télécharge et renvoie un média vers la destination en préservant le type."""
    dest_entity = await get_dest_entity()
    if not dest_entity:
        logger.warning("Destination non configurée ou introuvable")
        return False

    caption = caption_override if caption_override is not None else (message.text or "")

    try:
        async with DOWNLOAD_SEMAPHORE:
            media_bytes = await user_client.download_media(message, bytes)

        if media_bytes is None:
            logger.warning(f"Téléchargement vide pour msg_id={message.id}")
            return False

        file_obj, extra_kwargs = _media_to_file(message, media_bytes)
        if file_obj is None:
            return False

        kwargs = dict(file=file_obj, caption=caption, **extra_kwargs)

        async with UPLOAD_SEMAPHORE:
            try:
                await user_client.send_file(dest_entity, **kwargs)
            except Exception as e_user:
                logger.warning(f"user_client.send_file échoué ({e_user}), essai bot_client…")
                try:
                    await bot_client.send_file(dest_entity, **kwargs)
                except Exception as e_bot:
                    logger.error(f"bot_client.send_file échoué aussi: {e_bot}")
                    return False

        return True

    except FloodWaitError as e:
        logger.warning(f"FloodWait envoi: attente de {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
        return await send_media_to_destination(message, caption_override)
    except MediaEmptyError:
        logger.warning("Média vide, ignoré")
        return False
    except Exception as e:
        logger.error(f"Erreur envoi média (msg_id={getattr(message,'id','?')}): {e}")
        return False


async def send_album_to_destination(messages: list, source_name: str) -> int:
    """Télécharge tous les médias en parallèle et les envoie en un seul album groupé."""
    dest_entity = await get_dest_entity()
    if not dest_entity:
        return 0

    async def dl_one(msg):
        try:
            async with DOWNLOAD_SEMAPHORE:
                return await user_client.download_media(msg, bytes)
        except Exception as e:
            logger.warning(f"Échec dl album msg_id={msg.id}: {e}")
            return None

    # Téléchargement parallèle de tous les fichiers de l'album
    results = await asyncio.gather(*[dl_one(m) for m in messages])

    files = []
    has_video = False
    for msg, media_bytes in zip(messages, results):
        if media_bytes is None:
            continue
        file_obj, extra = _media_to_file(msg, media_bytes)
        if file_obj is not None:
            files.append(file_obj)
            if extra.get("supports_streaming"):
                has_video = True

    if not files:
        return 0

    caption = f"📺 Source: {source_name}\n📅 {now_paris().strftime('%d/%m/%Y à %H:%M')}"
    captions = [caption] + [""] * (len(files) - 1)

    # Note: supports_streaming n'est pas compatible avec les listes dans Telethon —
    # le .name sur chaque BytesIO suffit pour que Telegram détecte le type.
    send_kwargs = dict(file=files, caption=captions)

    try:
        await user_client.send_file(dest_entity, **send_kwargs)
        return len(files)
    except Exception as e_user:
        logger.warning(f"user_client album échoué ({e_user}), essai bot_client…")
        try:
            await bot_client.send_file(dest_entity, **send_kwargs)
            return len(files)
        except Exception as e_bot:
            logger.error(f"Erreur envoi album bot_client: {e_bot}")
            # Fallback : envoi un par un
            count = 0
            for msg, mb in zip(messages, results):
                if mb is None:
                    continue
                ok = await send_media_to_destination(msg)
                if ok:
                    count += 1
            return count


async def flush_album(grouped_id: int):
    """Attend ALBUM_WAIT s pour collecter tous les médias du pack, puis envoie d'un coup."""
    await asyncio.sleep(ALBUM_WAIT)
    items = _album_buffer.pop(grouped_id, [])
    _album_flush_tasks.pop(grouped_id, None)
    if not items:
        return

    items.sort(key=lambda x: x[0].id)
    source_name = items[0][1]
    chat_id = items[0][2]

    # Déduplication
    new_msgs = []
    for msg, _sname, cid in items:
        msg_key = (cid, msg.id)
        if msg_key in _seen_messages:
            continue
        _seen_messages.add(msg_key)
        if msg.id in data.history_ids:
            continue
        data.history_ids.add(msg.id)
        new_msgs.append(msg)

    if not new_msgs:
        return

    data.save()
    logger.info(f"Album groupé {grouped_id}: {len(new_msgs)} médias depuis {source_name}")

    count = await send_album_to_destination(new_msgs, source_name)
    if count > 0:
        data.increment_stats(count)
        await notify_owner(
            f"✅ Album transféré ! ({count} médias)\n"
            f"📚 Depuis **{source_name}**\n"
            f"➡️ {data.destination}\n"
            f"⏰ {now_paris().strftime('%d/%m/%Y à %H:%M')}"
        )


def _progress_bar(done: int, total: int, width: int = 16) -> str:
    """Génère une barre de progression style ████░░░░"""
    if total == 0:
        return "░" * width
    filled = int(width * done / total)
    return "█" * filled + "░" * (width - filled)


async def process_message_queue(
    messages: list,
    source_name: str = "",
    progress_callback=None,
) -> tuple[int, int]:
    """
    Traite une liste de messages en parallèle par lots.
    - progress_callback(sent, failed, total, current_type) : appelé après chaque envoi
    - Suivi en direct via callback, mise à jour Telegram toutes les ~3s max
    """
    BATCH_SIZE = 8
    sent = 0
    failed = 0
    last_update = 0.0

    media_messages = [
        m for m in messages
        if m.media and isinstance(m.media, (MessageMediaPhoto, MessageMediaDocument))
    ]
    total = len(media_messages)

    logger.info(f"Traitement de {total} médias depuis {source_name}")

    start_time = asyncio.get_event_loop().time()

    async def send_and_report(msg):
        nonlocal sent, failed, last_update
        result = await send_media_to_destination(msg)
        media_type = "📸" if isinstance(msg.media, MessageMediaPhoto) else "🎬"
        if result is True:
            sent += 1
        else:
            failed += 1
        # Mise à jour toutes les 3 secondes max pour éviter FloodWait
        now = asyncio.get_event_loop().time()
        if progress_callback and (now - last_update) >= 3.0:
            last_update = now
            elapsed = max(now - start_time, 0.1)
            speed = (sent + failed) / elapsed * 60  # médias/min
            remaining = (total - sent - failed)
            eta_s = int(remaining / max(speed / 60, 0.01))
            eta_str = (
                f"{eta_s // 60}m {eta_s % 60}s" if eta_s >= 60
                else f"{eta_s}s"
            )
            await progress_callback(sent, failed, total, speed, eta_str)

    for i in range(0, total, BATCH_SIZE):
        batch = media_messages[i:i + BATCH_SIZE]
        await asyncio.gather(*[send_and_report(msg) for msg in batch], return_exceptions=True)
        if i + BATCH_SIZE < total:
            await asyncio.sleep(0.3)

    # Mise à jour finale
    if progress_callback:
        await progress_callback(sent, failed, total, 0, "0s")

    data.increment_stats(sent)
    logger.info(f"Lot terminé: {sent} envoyés, {failed} échecs")
    return sent, failed


def is_owner(sender_id: int) -> bool:
    return sender_id == OWNER_ID


async def notify_owner(text: str):
    try:
        await bot_client.send_message(OWNER_ID, text, parse_mode="md")
    except Exception as e:
        logger.error(f"Impossible de notifier le propriétaire: {e}")


def setup_user_handlers():
    """Surveille les nouveaux messages dans les canaux sources."""

    @user_client.on(events.NewMessage())
    async def on_new_message(event):
        if data.paused or not data.destination_id or not data.source_channels:
            return

        try:
            chat = await event.get_chat()
            chat_id = getattr(chat, "id", None)
            if chat_id is None:
                return

            source_ids = [ch["id"] for ch in data.source_channels]
            if chat_id not in source_ids:
                return

            msg = event.message
            if not msg.media or not isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
                return

            source_name = next(
                (ch["name"] for ch in data.source_channels if ch["id"] == chat_id),
                str(chat_id),
            )

            # ── Album (pack de médias) → buffer + flush différé ───────────
            if msg.grouped_id:
                gid = msg.grouped_id
                if gid not in _album_buffer:
                    _album_buffer[gid] = []
                _album_buffer[gid].append((msg, source_name, chat_id))
                # Annuler le flush précédent et en créer un nouveau
                if gid in _album_flush_tasks and not _album_flush_tasks[gid].done():
                    _album_flush_tasks[gid].cancel()
                _album_flush_tasks[gid] = asyncio.create_task(flush_album(gid))
                return

            # ── Média seul ────────────────────────────────────────────────
            msg_key = (chat_id, msg.id)
            if msg_key in _seen_messages:
                return
            _seen_messages.add(msg_key)

            if msg.id in data.history_ids:
                return
            data.history_ids.add(msg.id)
            data.save()

            caption_parts = []
            if msg.text:
                caption_parts.append(msg.text)
            caption_parts.append(f"\n📺 Source: {source_name}")
            caption_parts.append(f"📅 {now_paris().strftime('%d/%m/%Y à %H:%M')}")
            caption = "\n".join(caption_parts)

            success = await send_media_to_destination(msg, caption)
            media_type = "📸 Photo" if isinstance(msg.media, MessageMediaPhoto) else "🎬 Vidéo"

            if success:
                logger.info(f"Média transféré depuis {source_name} (msg_id={msg.id})")
                await notify_owner(
                    f"✅ Transfert réussi !\n"
                    f"{media_type} de **{source_name}**\n"
                    f"➡️ {data.destination}\n"
                    f"⏰ {now_paris().strftime('%d/%m/%Y à %H:%M')}"
                )
            else:
                logger.warning(f"Échec transfert depuis {source_name} (msg_id={msg.id})")

        except Exception as e:
            logger.error(f"Erreur handler new message: {e}")


def setup_bot_handlers():
    """
    Enregistre toutes les commandes du bot.
    - incoming=True : ignore les messages envoyés PAR le bot (anti-double réponse)
    - Patterns ancrés (^...$) : évite les faux matchs sur des sous-chaînes
    - from_users=OWNER_ID : seul le propriétaire peut déclencher les commandes
    """
    OWN = [OWNER_ID]

    @bot_client.on(events.NewMessage(
        pattern=r"^/start(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_start(event):
        await event.respond(
            "🤖 **Bot de transfert de médias**\n\n"
            "Utilise le menu `/` pour voir toutes les commandes."
        )

    @bot_client.on(events.NewMessage(
        pattern=r"^/help(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_help(event):
        await event.respond(
            "🤖 **Commandes disponibles**\n\n"
            "**📡 Canaux**\n"
            "`/addcanal` `<lien>` — ajouter un canal source\n"
            "`/removecanal` `<n°>` — supprimer un canal source\n"
            "`/canaux` — voir les canaux surveillés\n"
            "`/setdestination` `<lien>` — définir la destination\n\n"
            "**⚙️ Contrôle**\n"
            "`/pause` — mettre en pause\n"
            "`/resume` — reprendre\n"
            "`/clear` — effacer l'historique des IDs\n\n"
            "**📥 Historique**\n"
            "`/gethistory` `<lien>` — récupérer tout l'historique\n\n"
            "**📊 Infos**\n"
            "`/status` — état du bot\n"
            "`/stats` — statistiques\n"
            "`/help` — cette aide"
        )

    @bot_client.on(events.NewMessage(
        pattern=r"^/addcanal(@\w+)?\s+(.+)$", incoming=True, from_users=OWN
    ))
    async def cmd_addcanal(event):
        link = event.pattern_match.group(2).strip()
        try:
            entity = await resolve_channel(link)
            channel_id = entity.id
            channel_name = getattr(entity, "title", link)
            if any(ch["id"] == channel_id for ch in data.source_channels):
                await event.respond(f"⚠️ **{channel_name}** est déjà dans la liste.")
                return
            data.source_channels.append({"id": channel_id, "name": channel_name, "link": link})
            data.save()
            await event.respond(f"✅ Canal ajouté : **{channel_name}**")
            logger.info(f"Canal source ajouté: {channel_name} ({channel_id})")
        except ValueError as e:
            await event.respond(f"❌ {e}")
        except Exception as e:
            await event.respond(f"❌ Erreur : {e}")

    @bot_client.on(events.NewMessage(
        pattern=r"^/removecanal(@\w+)?\s+(\d+)$", incoming=True, from_users=OWN
    ))
    async def cmd_removecanal(event):
        num = int(event.pattern_match.group(2)) - 1
        if 0 <= num < len(data.source_channels):
            removed = data.source_channels.pop(num)
            data.save()
            await event.respond(f"🗑️ Canal supprimé : **{removed['name']}**")
        else:
            await event.respond("❌ Numéro invalide. Utilise `/canaux` pour voir la liste.")

    @bot_client.on(events.NewMessage(
        pattern=r"^/canaux(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_canaux(event):
        if not data.source_channels:
            await event.respond(
                "📋 Aucun canal source.\nUtilise `/addcanal <lien>` pour en ajouter."
            )
            return
        lines = ["📋 **Canaux surveillés :**\n"]
        for i, ch in enumerate(data.source_channels, 1):
            lines.append(f"{i}. **{ch['name']}**")
        lines.append(f"\n📍 Destination : {data.destination or 'non définie'}")
        await event.respond("\n".join(lines))

    @bot_client.on(events.NewMessage(
        pattern=r"^/setdestination(@\w+)?\s+(.+)$", incoming=True, from_users=OWN
    ))
    async def cmd_setdestination(event):
        link = event.pattern_match.group(2).strip()
        try:
            entity = await resolve_channel(link)
            channel_name = getattr(entity, "title", link)
            data.destination = link
            data.destination_id = entity.id
            data.save()
            invalidate_dest_cache()
            await event.respond(f"✅ Destination : **{channel_name}**")
            logger.info(f"Destination définie: {channel_name} (id={entity.id})")
        except ValueError as e:
            await event.respond(f"❌ {e}")
        except Exception as e:
            await event.respond(f"❌ Erreur : {e}")

    @bot_client.on(events.NewMessage(
        pattern=r"^/pause(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_pause(event):
        data.paused = True
        data.save()
        await event.respond("⏸️ **Mis en pause.** Les transferts sont suspendus.")

    @bot_client.on(events.NewMessage(
        pattern=r"^/resume(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_resume(event):
        data.paused = False
        data.save()
        await event.respond("▶️ **Repris !** Les transferts recommencent.")

    @bot_client.on(events.NewMessage(
        pattern=r"^/clear(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_clear(event):
        count = len(data.history_ids)
        data.history_ids.clear()
        data.save()
        await event.respond(f"🗑️ Historique effacé ({count} IDs supprimés).")

    @bot_client.on(events.NewMessage(
        pattern=r"^/status(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_status(event):
        state = "⏸️ En pause" if data.paused else "▶️ Actif"
        dest = data.destination or "Non configurée"
        src_list = "\n".join(
            f"  • {ch['name']}" for ch in data.source_channels
        ) or "  Aucun"
        await event.respond(
            f"📊 **État du bot**\n\n"
            f"État : {state}\n"
            f"Destination : `{dest}`\n"
            f"Sources ({len(data.source_channels)}) :\n{src_list}\n"
            f"IDs suivis : {len(data.history_ids)}"
        )

    @bot_client.on(events.NewMessage(
        pattern=r"^/stats(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_stats(event):
        data.reset_stats_if_new_day()
        await event.respond(
            f"📈 **Statistiques**\n\n"
            f"Aujourd'hui : **{data.stats.get('today', 0)}** médias\n"
            f"Total : **{data.stats.get('total', 0)}** médias"
        )

    @bot_client.on(events.NewMessage(
        pattern=r"^/gethistory(@\w+)?(?:\s+(.+))?$", incoming=True, from_users=OWN
    ))
    async def cmd_gethistory(event):
        if not data.destination:
            await event.respond(
                "❌ Aucune destination configurée.\n"
                "Utilise `/setdestination <lien>` d'abord."
            )
            return

        link_arg = event.pattern_match.group(2)
        target_link = (link_arg.strip() if link_arg else None) or (
            data.source_channels[0]["link"] if data.source_channels else None
        )
        if not target_link:
            await event.respond("❌ Spécifie un canal : `/gethistory @canal`")
            return

        status_msg = await event.respond(
            "🔍 Connexion au canal en cours..."
        )

        try:
            entity = await resolve_channel(target_link)
        except ValueError as e:
            await status_msg.edit(f"❌ {e}")
            return

        source_name = getattr(entity, "title", target_link)
        all_messages = []
        offset_id = 0
        total_fetched = 0
        last_scan_update = 0.0

        await status_msg.edit(
            f"📡 **Scan de l'historique**\n"
            f"📺 Canal : **{source_name}**\n\n"
            f"⏳ Récupération des messages..."
        )

        # ── Phase 1 : scan de l'historique ──────────────────────────────────
        while True:
            try:
                history = await user_client(
                    GetHistoryRequest(
                        peer=entity,
                        limit=100,
                        offset_date=None,
                        offset_id=offset_id,
                        max_id=0,
                        min_id=0,
                        add_offset=0,
                        hash=0,
                    )
                )
            except FloodWaitError as e:
                await asyncio.sleep(e.seconds + 1)
                continue
            except Exception as e:
                await status_msg.edit(f"❌ Erreur récupération historique : {e}")
                return

            if not history.messages:
                break

            for msg in history.messages:
                if msg.media and isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
                    all_messages.append(msg)

            total_fetched += len(history.messages)
            offset_id = history.messages[-1].id

            now = asyncio.get_event_loop().time()
            if now - last_scan_update >= 2.0:
                last_scan_update = now
                await status_msg.edit(
                    f"📡 **Scan de l'historique**\n"
                    f"📺 Canal : **{source_name}**\n\n"
                    f"🔎 Messages parcourus : `{total_fetched}`\n"
                    f"🎬 Médias trouvés : `{len(all_messages)}`\n"
                    f"⏳ Scan en cours..."
                )

            if len(history.messages) < 100:
                break
            await asyncio.sleep(0.3)

        if not all_messages:
            await status_msg.edit(
                f"📭 **Aucun média trouvé**\n\n"
                f"Canal : **{source_name}**\n"
                f"Messages parcourus : {total_fetched}"
            )
            return

        new_messages = [m for m in all_messages if m.id not in data.history_ids]
        skipped = len(all_messages) - len(new_messages)
        total_new = len(new_messages)

        if total_new == 0:
            await status_msg.edit(
                f"✅ **Déjà tout envoyé !**\n\n"
                f"Canal : **{source_name}**\n"
                f"Médias trouvés : {len(all_messages)}\n"
                f"Déjà envoyés : {skipped}"
            )
            return

        await status_msg.edit(
            f"📤 **Envoi en cours**\n"
            f"📺 Canal : **{source_name}**\n\n"
            f"`{'░' * 16}` 0/{total_new}\n"
            f"✅ Envoyés : 0  ❌ Échecs : 0\n"
            f"⚡ Vitesse : calcul...\n"
            f"⏱ Temps restant : calcul..."
        )

        # ── Phase 2 : envoi avec suivi en direct ────────────────────────────
        async def live_progress(s, f, total, speed, eta):
            bar = _progress_bar(s + f, total)
            speed_str = f"{speed:.1f} médias/min" if speed > 0 else "calcul..."
            await status_msg.edit(
                f"📤 **Envoi en cours**\n"
                f"📺 Canal : **{source_name}**\n\n"
                f"`{bar}` {s + f}/{total}\n"
                f"✅ Envoyés : **{s}**  ❌ Échecs : **{f}**\n"
                f"⚡ Vitesse : {speed_str}\n"
                f"⏱ Temps restant : ~{eta}"
            )

        sent, failed = await process_message_queue(
            new_messages, source_name, progress_callback=live_progress
        )

        for msg in new_messages:
            data.history_ids.add(msg.id)
        data.save()

        bar_done = _progress_bar(total_new, total_new)
        await status_msg.edit(
            f"🏁 **Historique terminé !**\n"
            f"📺 Canal : **{source_name}**\n\n"
            f"`{bar_done}` {total_new}/{total_new}\n\n"
            f"✅ Envoyés : **{sent}**\n"
            f"❌ Échecs : **{failed}**\n"
            f"⏭️ Déjà envoyés : {skipped}\n"
            f"📊 Total trouvés : {len(all_messages)}"
        )


def auto_push_to_github():
    """Pousse bot.py vers GitHub automatiquement au démarrage si GITHUB_TOKEN est défini."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        return
    import base64, json, urllib.request, urllib.error
    owner, repo = "kns336cne", "bot-telegram-"
    filepath = "telegram-bot/bot.py"
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{filepath}"
    headers = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    }
    try:
        script_path = os.path.join(os.path.dirname(__file__), "bot.py")
        with open(script_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode()
        # Récupérer le sha actuel
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req) as r:
                existing = json.loads(r.read())
            sha = existing.get("sha")
        except urllib.error.HTTPError:
            sha = None
        payload = {"message": "Auto-sync bot.py", "content": content_b64}
        if sha:
            payload["sha"] = sha
        req2 = urllib.request.Request(
            url, data=json.dumps(payload).encode(), headers=headers, method="PUT"
        )
        with urllib.request.urlopen(req2) as r:
            code = r.status
        logger.info(f"GitHub auto-sync: bot.py {'mis à jour' if code == 200 else 'créé'} (HTTP {code})")
    except Exception as e:
        logger.warning(f"GitHub auto-sync échoué (non bloquant): {e}")


async def register_bot_commands():
    """Enregistre le menu de commandes visible dans Telegram (style BotFather)."""
    commands = [
        BotCommand(command="status",        description="État du bot"),
        BotCommand(command="canaux",        description="Canaux surveillés"),
        BotCommand(command="addcanal",      description="Ajouter un canal source"),
        BotCommand(command="removecanal",   description="Supprimer un canal source"),
        BotCommand(command="setdestination",description="Définir le canal de destination"),
        BotCommand(command="gethistory",    description="Récupérer tout l'historique"),
        BotCommand(command="pause",         description="Mettre en pause"),
        BotCommand(command="resume",        description="Reprendre"),
        BotCommand(command="stats",         description="Statistiques"),
        BotCommand(command="clear",         description="Effacer l'historique des IDs"),
        BotCommand(command="help",          description="Aide"),
    ]
    try:
        await bot_client(SetBotCommandsRequest(
            scope=BotCommandScopeDefault(),
            lang_code="fr",
            commands=commands,
        ))
        logger.info(f"Menu de commandes enregistré ({len(commands)} commandes)")
    except Exception as e:
        logger.warning(f"Impossible d'enregistrer les commandes: {e}")


async def main():
    global user_client, bot_client

    logger.info("Démarrage du bot...")
    auto_push_to_github()

    if not SESSION_STRING:
        raise RuntimeError(
            "SESSION_STRING manquant ! Génère-le avec generate_session.py "
            "et ajoute-le comme variable d'environnement SESSION_STRING."
        )

    user_client = TelegramClient(
        StringSession(SESSION_STRING),
        API_ID,
        API_HASH,
        connection_retries=-1,
        retry_delay=5,
    )

    bot_client = TelegramClient(
        StringSession(),
        API_ID,
        API_HASH,
        connection_retries=-1,
        retry_delay=5,
    )

    try:
        # Connexion manuelle du client utilisateur (évite le prompt interactif)
        await user_client.connect()
        if not await user_client.is_user_authorized():
            raise RuntimeError(
                "SESSION_STRING invalide ou expiré. "
                "Régénère-le avec generate_session.py."
            )
        logger.info("Client utilisateur connecté avec SESSION_STRING")

        # Démarrage du client bot
        await bot_client.start(bot_token=BOT_TOKEN)
        logger.info("Client bot démarré")

        setup_user_handlers()
        setup_bot_handlers()
        await register_bot_commands()

        me = await user_client.get_me()
        bot_me = await bot_client.get_me()
        logger.info(f"Client utilisateur: {me.first_name} (@{getattr(me, 'username', 'N/A')})")
        logger.info(f"Client bot: @{bot_me.username}")

        await notify_owner(
            f"🚀 **Bot démarré !**\n\n"
            f"👤 Compte : {me.first_name}\n"
            f"🤖 Bot : @{bot_me.username}\n"
            f"📡 Canaux surveillés : {len(data.source_channels)}\n"
            f"📍 Destination : {data.destination or 'Non configurée'}"
        )

        logger.info("Bot opérationnel, en attente de nouveaux messages...")
        await asyncio.gather(
            user_client.run_until_disconnected(),
            bot_client.run_until_disconnected(),
        )

    finally:
        if user_client.is_connected():
            await user_client.disconnect()
        if bot_client.is_connected():
            await bot_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
