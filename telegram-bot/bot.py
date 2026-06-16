"""
Telegram Bot - Transfert de médias entre canaux (y compris canaux restreints)
Utilise Telethon (session utilisateur) pour accéder aux canaux sans forwarding
et un client bot pour les commandes.

Fonctionnalités :
- Plusieurs canaux sources
- Plusieurs canaux de réception
- Routage par source : chaque source → sa destination + filtre (photo/vidéo/tout)
- Transfert d'albums groupés
- Récupération de l'historique avec suivi en direct
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

API_ID    = int(os.environ["API_ID"])
API_HASH  = os.environ["API_HASH"]
SESSION_STRING = os.environ.get("SESSION_STRING", "")
BOT_TOKEN = os.environ["BOT_TOKEN"]
OWNER_ID  = int(os.environ["OWNER_ID"])

DATA_FILE = "bot_data.json"

UPLOAD_SEMAPHORE   = asyncio.Semaphore(5)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(8)

_seen_messages: set[tuple[int, int]] = set()

# Cache entités : channel_id → entity
_entity_cache: dict[int, object] = {}

# Buffer albums : grouped_id → [(message, source_name, chat_id)]
_album_buffer: dict[int, list] = {}
_album_flush_tasks: dict[int, asyncio.Task] = {}
ALBUM_WAIT = 0.8

user_client: TelegramClient = None
bot_client:  TelegramClient = None


# ── Données persistantes ──────────────────────────────────────────────────────

class BotData:
    def __init__(self):
        self.source_channels: list[dict] = []
        # Nouveau : liste de canaux de réception
        self.destination_channels: list[dict] = []
        # Ancien champ (rétro-compatibilité) — pointe sur destination_channels[0]
        self.destination: str | None = None
        self.destination_id: int | None = None
        # Règles de routage : source_id → dest_id + filtre
        # {"source_id": int, "dest_id": int, "filter": "all"|"photo"|"video"}
        self.routes: list[dict] = []
        self.paused: bool = False
        self.stats: dict = {"today": 0, "total": 0, "date": str(datetime.now().date())}
        self.history_ids: set[int] = set()
        self.invite_cache: dict[str, int] = {}
        self._load()

    def _load(self):
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, "r") as f:
                    raw = json.load(f)
                self.source_channels      = raw.get("source_channels", [])
                self.destination_channels = raw.get("destination_channels", [])
                self.destination          = raw.get("destination")
                self.destination_id       = raw.get("destination_id")
                self.routes               = raw.get("routes", [])
                self.paused               = raw.get("paused", False)
                self.stats                = raw.get("stats", self.stats)
                self.history_ids          = set(raw.get("history_ids", []))
                self.invite_cache         = raw.get("invite_cache", {})

                # Migration : si ancien destination_id présent mais pas de destination_channels
                if self.destination_id and not self.destination_channels:
                    self.destination_channels = [{
                        "id":   self.destination_id,
                        "name": self.destination or str(self.destination_id),
                        "link": self.destination or "",
                    }]

                logger.info(
                    f"Données chargées: {len(self.source_channels)} sources, "
                    f"{len(self.destination_channels)} destinations, "
                    f"{len(self.routes)} règles"
                )
            except Exception as e:
                logger.error(f"Erreur chargement données: {e}")

    def save(self):
        # Sync ancien champ pour rétro-compatibilité
        if self.destination_channels:
            self.destination    = self.destination_channels[0].get("link")
            self.destination_id = self.destination_channels[0].get("id")
        try:
            with open(DATA_FILE, "w") as f:
                json.dump(
                    {
                        "source_channels":      self.source_channels,
                        "destination_channels": self.destination_channels,
                        "destination":          self.destination,
                        "destination_id":       self.destination_id,
                        "routes":               self.routes,
                        "paused":               self.paused,
                        "stats":                self.stats,
                        "history_ids":          list(self.history_ids),
                        "invite_cache":         self.invite_cache,
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
            self.stats["date"]  = today
            self.save()

    def increment_stats(self, count=1):
        self.reset_stats_if_new_day()
        self.stats["today"] = self.stats.get("today", 0) + count
        self.stats["total"] = self.stats.get("total", 0) + count
        self.save()

    # ── Routage ──────────────────────────────────────────────────────────────

    def get_routes_for_source(self, source_id: int) -> list[dict]:
        """Retourne toutes les règles qui s'appliquent à ce canal source."""
        return [r for r in self.routes if r["source_id"] == source_id]

    def get_default_dest(self) -> dict | None:
        """Destination par défaut (première de la liste)."""
        return self.destination_channels[0] if self.destination_channels else None

    def get_dest_by_id(self, dest_id: int) -> dict | None:
        return next((d for d in self.destination_channels if d["id"] == dest_id), None)


data = BotData()


# ── Extension d'une map MIME ──────────────────────────────────────────────────

_EXT_MAP = {
    "video/mp4": "mp4",       "video/quicktime": "mov",
    "video/x-matroska": "mkv","video/webm": "webm",
    "video/avi": "avi",       "video/3gpp": "3gp",
    "image/jpeg": "jpg",      "image/png": "png",
    "image/gif": "gif",       "image/webp": "webp",
}


# ── Helpers médias ────────────────────────────────────────────────────────────

def _get_media_kind(message) -> str:
    """Retourne 'photo', 'video', 'gif', 'image' ou 'other'."""
    if isinstance(message.media, MessageMediaPhoto):
        return "photo"
    if isinstance(message.media, MessageMediaDocument):
        doc   = message.media.document
        mime  = doc.mime_type or ""
        attrs = doc.attributes or []
        if mime == "image/gif" or any(type(a).__name__ == "DocumentAttributeAnimated" for a in attrs):
            return "gif"
        if mime.startswith("video/") or any(type(a).__name__ == "DocumentAttributeVideo" for a in attrs):
            return "video"
        if mime.startswith("image/"):
            return "image"
        return "other"
    return "other"


def _filter_matches(media_kind: str, route_filter: str) -> bool:
    """Vérifie si un média correspond au filtre d'une règle."""
    if route_filter == "all":
        return True
    if route_filter == "photo":
        return media_kind in ("photo", "image", "gif")
    if route_filter == "video":
        return media_kind == "video"
    return True


def _media_to_file(message, media_bytes: bytes):
    """Prépare (file_obj, extra_kwargs) pour send_file selon le type de média.
    Retourne (None, None) si type non supporté.
    """
    if isinstance(message.media, MessageMediaPhoto):
        bio = BytesIO(media_bytes)
        bio.name = "photo.jpg"
        return bio, {"force_document": False}

    if isinstance(message.media, MessageMediaDocument):
        doc   = message.media.document
        mime  = doc.mime_type or ""
        attrs = doc.attributes or []
        is_video = mime.startswith("video/") or any(
            type(a).__name__ == "DocumentAttributeVideo" for a in attrs
        )
        is_gif   = mime == "image/gif" or any(
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


# ── Cache entités ─────────────────────────────────────────────────────────────

async def get_entity_cached(channel_id: int):
    """Retourne l'entité Telethon depuis le cache ou via get_entity()."""
    if channel_id in _entity_cache:
        return _entity_cache[channel_id]
    try:
        entity = await user_client.get_entity(PeerChannel(channel_id))
        _entity_cache[channel_id] = entity
        return entity
    except Exception as e:
        logger.error(f"Impossible de récupérer l'entité id={channel_id}: {e}")
        return None


def invalidate_entity_cache(channel_id: int | None = None):
    if channel_id is None:
        _entity_cache.clear()
    else:
        _entity_cache.pop(channel_id, None)


# ── Résolution de canal ───────────────────────────────────────────────────────

async def resolve_channel(identifier: str):
    """
    Résout un canal depuis un lien public, username ou lien d'invitation privé.
    """
    identifier = identifier.strip()
    if "t.me/" in identifier:
        identifier = identifier.split("t.me/")[-1].strip("/")

    if identifier.startswith("+"):
        invite_hash = identifier[1:]

        def _extract_hash(link: str) -> str:
            return link.split("+")[-1].strip("/") if "+" in link else ""

        if invite_hash in data.invite_cache:
            try:
                return await user_client.get_entity(PeerChannel(data.invite_cache[invite_hash]))
            except Exception:
                del data.invite_cache[invite_hash]
                data.save()

        known_links = [(ch.get("link", ""), ch["id"]) for ch in data.source_channels]
        known_links += [(d.get("link", ""), d["id"]) for d in data.destination_channels]
        for link, cid in known_links:
            if _extract_hash(link) == invite_hash:
                try:
                    entity = await user_client.get_entity(PeerChannel(cid))
                    data.invite_cache[invite_hash] = cid
                    data.save()
                    return entity
                except Exception:
                    pass

        try:
            joined = await user_client(ImportChatInviteRequest(hash=invite_hash))
            entity = joined.chats[0]
            data.invite_cache[invite_hash] = entity.id
            data.save()
            return entity
        except UserAlreadyParticipantError:
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
                    f"pendant encore **{mins}min {secs}s**.\nRéessaie dans {mins} minutes."
                )
            except Exception:
                pass
            raise ValueError(
                "⚠️ Tu es déjà dans ce canal mais il est introuvable.\n"
                "Réessaie dans quelques minutes."
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

    if not identifier.startswith("@"):
        identifier = "@" + identifier
    try:
        return await user_client.get_entity(identifier)
    except Exception:
        try:
            return await user_client.get_entity(identifier.lstrip("@"))
        except Exception as e:
            raise ValueError(f"Impossible de résoudre le canal `{identifier}`: {e}")


# ── Envoi d'un média vers une destination ────────────────────────────────────

async def send_media_to_destination(
    message,
    dest_id: int,
    caption_override: str = None,
) -> bool:
    """Télécharge et renvoie un média vers la destination donnée."""
    dest_entity = await get_entity_cached(dest_id)
    if not dest_entity:
        logger.warning(f"Destination id={dest_id} introuvable")
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
        return await send_media_to_destination(message, dest_id, caption_override)
    except MediaEmptyError:
        logger.warning("Média vide, ignoré")
        return False
    except Exception as e:
        logger.error(f"Erreur envoi média (msg_id={getattr(message,'id','?')}): {e}")
        return False


# ── Envoi d'un album vers une destination ────────────────────────────────────

async def send_album_to_destination(
    messages: list,
    source_name: str,
    dest_id: int,
    media_filter: str = "all",
) -> int:
    """Télécharge et envoie un album filtré par type vers la destination donnée."""
    dest_entity = await get_entity_cached(dest_id)
    if not dest_entity:
        return 0

    # Filtrer les messages selon le type
    filtered = [m for m in messages if _filter_matches(_get_media_kind(m), media_filter)]
    if not filtered:
        return 0

    async def dl_one(msg):
        try:
            async with DOWNLOAD_SEMAPHORE:
                return await user_client.download_media(msg, bytes)
        except Exception as e:
            logger.warning(f"Échec dl album msg_id={msg.id}: {e}")
            return None

    results = await asyncio.gather(*[dl_one(m) for m in filtered])

    files = []
    for msg, media_bytes in zip(filtered, results):
        if media_bytes is None:
            continue
        file_obj, _ = _media_to_file(msg, media_bytes)
        if file_obj is not None:
            files.append(file_obj)

    if not files:
        return 0

    caption  = f"📺 Source: {source_name}\n📅 {now_paris().strftime('%d/%m/%Y à %H:%M')}"
    captions = [caption] + [""] * (len(files) - 1)
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
            count = 0
            for msg, mb in zip(filtered, results):
                if mb is None:
                    continue
                ok = await send_media_to_destination(msg, dest_id)
                if ok:
                    count += 1
            return count


# ── Routage d'un message individuel ──────────────────────────────────────────

async def route_and_send(message, source_id: int, source_name: str, caption: str) -> int:
    """
    Détermine la/les destinations selon les règles, filtre le type de média,
    envoie et retourne le nombre d'envois réussis.
    """
    media_kind = _get_media_kind(message)
    routes     = data.get_routes_for_source(source_id)
    sent       = 0

    if routes:
        for route in routes:
            if not _filter_matches(media_kind, route["filter"]):
                continue
            ok = await send_media_to_destination(message, route["dest_id"], caption)
            if ok:
                sent += 1
    else:
        # Pas de règle : utilise la destination par défaut
        default = data.get_default_dest()
        if default:
            ok = await send_media_to_destination(message, default["id"], caption)
            if ok:
                sent += 1

    return sent


async def route_and_send_album(messages: list, source_id: int, source_name: str) -> int:
    """Routage et envoi d'un album groupé."""
    routes = data.get_routes_for_source(source_id)
    sent   = 0

    if routes:
        for route in routes:
            count = await send_album_to_destination(
                messages, source_name, route["dest_id"], route["filter"]
            )
            sent += count
    else:
        default = data.get_default_dest()
        if default:
            count = await send_album_to_destination(
                messages, source_name, default["id"], "all"
            )
            sent += count

    return sent


# ── Flush album ───────────────────────────────────────────────────────────────

async def flush_album(grouped_id: int):
    await asyncio.sleep(ALBUM_WAIT)
    items = _album_buffer.pop(grouped_id, [])
    _album_flush_tasks.pop(grouped_id, None)
    if not items:
        return

    items.sort(key=lambda x: x[0].id)
    source_name = items[0][1]
    source_id   = items[0][2]

    new_msgs = []
    for msg, _sname, sid in items:
        msg_key = (sid, msg.id)
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

    count = await route_and_send_album(new_msgs, source_id, source_name)
    if count > 0:
        data.increment_stats(count)
        await notify_owner(
            f"✅ Album transféré ! ({count} médias)\n"
            f"📚 Depuis **{source_name}**\n"
            f"⏰ {now_paris().strftime('%d/%m/%Y à %H:%M')}"
        )


# ── Traitement de la file d'historique ───────────────────────────────────────

def _progress_bar(done: int, total: int, width: int = 16) -> str:
    if total == 0:
        return "░" * width
    filled = int(width * done / total)
    return "█" * filled + "░" * (width - filled)


async def process_message_queue(
    messages: list,
    source_id: int,
    source_name: str = "",
    progress_callback=None,
) -> tuple[int, int]:
    BATCH_SIZE = 8
    sent       = 0
    failed     = 0
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
        caption_parts = []
        if msg.text:
            caption_parts.append(msg.text)
        caption_parts.append(f"\n📺 Source: {source_name}")
        caption_parts.append(f"📅 {now_paris().strftime('%d/%m/%Y à %H:%M')}")
        caption = "\n".join(caption_parts)

        count = await route_and_send(msg, source_id, source_name, caption)
        if count > 0:
            sent += 1
        else:
            failed += 1

        now = asyncio.get_event_loop().time()
        if progress_callback and (now - last_update) >= 3.0:
            last_update = now
            elapsed     = max(now - start_time, 0.1)
            speed       = (sent + failed) / elapsed * 60
            remaining   = total - sent - failed
            eta_s       = int(remaining / max(speed / 60, 0.01))
            eta_str = (
                f"{eta_s // 60}m {eta_s % 60}s" if eta_s >= 60 else f"{eta_s}s"
            )
            await progress_callback(sent, failed, total, speed, eta_str)

    for i in range(0, total, BATCH_SIZE):
        batch = media_messages[i:i + BATCH_SIZE]
        await asyncio.gather(*[send_and_report(msg) for msg in batch], return_exceptions=True)
        if i + BATCH_SIZE < total:
            await asyncio.sleep(0.3)

    if progress_callback:
        await progress_callback(sent, failed, total, 0, "0s")

    data.increment_stats(sent)
    logger.info(f"Lot terminé: {sent} envoyés, {failed} échecs")
    return sent, failed


# ── Utilitaires bot ───────────────────────────────────────────────────────────

def is_owner(sender_id: int) -> bool:
    return sender_id == OWNER_ID


async def notify_owner(text: str):
    try:
        await bot_client.send_message(OWNER_ID, text, parse_mode="md")
    except Exception as e:
        logger.error(f"Impossible de notifier le propriétaire: {e}")


# ── Handler nouveaux messages (user_client) ───────────────────────────────────

def setup_user_handlers():
    @user_client.on(events.NewMessage())
    async def on_new_message(event):
        if data.paused or not data.destination_channels or not data.source_channels:
            return

        try:
            chat    = await event.get_chat()
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

            # ── Album ─────────────────────────────────────────────────────
            if msg.grouped_id:
                gid = msg.grouped_id
                if gid not in _album_buffer:
                    _album_buffer[gid] = []
                _album_buffer[gid].append((msg, source_name, chat_id))
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

            count = await route_and_send(msg, chat_id, source_name, caption)
            media_type = "📸 Photo" if isinstance(msg.media, MessageMediaPhoto) else "🎬 Vidéo"

            if count > 0:
                data.increment_stats(count)
                logger.info(f"Média transféré depuis {source_name} (msg_id={msg.id})")
                await notify_owner(
                    f"✅ Transfert réussi !\n"
                    f"{media_type} de **{source_name}**\n"
                    f"⏰ {now_paris().strftime('%d/%m/%Y à %H:%M')}"
                )
            else:
                logger.warning(f"Échec transfert depuis {source_name} (msg_id={msg.id})")

        except Exception as e:
            logger.error(f"Erreur handler new message: {e}")


# ── Commandes bot ─────────────────────────────────────────────────────────────

def setup_bot_handlers():
    OWN = [OWNER_ID]

    # ── /start ────────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/start(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_start(event):
        await event.respond(
            "🤖 **Bot de transfert de médias**\n\n"
            "Utilise le menu `/` pour voir toutes les commandes.\n\n"
            "💡 **Démarrage rapide :**\n"
            "1️⃣ `/addcanal <lien>` — ajouter une source\n"
            "2️⃣ `/adddestination <lien>` — ajouter une destination\n"
            "3️⃣ `/setroute <n° source> <n° dest> [photo|video|tout]` — règle de routage\n"
            "4️⃣ `/routes` — vérifier la configuration"
        )

    # ── /help ─────────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/help(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_help(event):
        await event.respond(
            "🤖 **Commandes disponibles**\n\n"
            "**📡 Sources**\n"
            "`/addcanal <lien>` — ajouter un canal source\n"
            "`/removecanal <n°>` — supprimer un canal source\n"
            "`/canaux` — voir les canaux sources\n\n"
            "**📬 Destinations**\n"
            "`/adddestination <lien>` — ajouter un canal de réception\n"
            "`/removedestination <n°>` — supprimer une destination\n"
            "`/destinations` — voir les destinations\n\n"
            "**🔀 Routage**\n"
            "`/setroute <n° source> <n° dest> [photo|video|tout]` — créer une règle\n"
            "`/removeroute <n°>` — supprimer une règle\n"
            "`/routes` — voir toutes les règles\n\n"
            "**⚙️ Contrôle**\n"
            "`/pause` — mettre en pause\n"
            "`/resume` — reprendre\n"
            "`/clear` — effacer l'historique des IDs\n\n"
            "**📥 Historique**\n"
            "`/gethistory <lien>` — récupérer tout l'historique\n\n"
            "**📊 Infos**\n"
            "`/status` — état du bot\n"
            "`/stats` — statistiques\n"
            "`/help` — cette aide\n\n"
            "**💡 Exemple de routage :**\n"
            "• Source 1 (Javana) → Dest 1 : vidéos uniquement\n"
            "• Source 1 (Javana) → Dest 2 : photos uniquement\n"
            "• Source 2 (Petit chat) → Dest 1 : tout\n"
            "→ `/setroute 1 1 video`\n"
            "→ `/setroute 1 2 photo`\n"
            "→ `/setroute 2 1 tout`"
        )

    # ── /addcanal ─────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/addcanal(@\w+)?\s+(.+)$", incoming=True, from_users=OWN
    ))
    async def cmd_addcanal(event):
        link = event.pattern_match.group(2).strip()
        try:
            entity     = await resolve_channel(link)
            channel_id = entity.id
            channel_name = getattr(entity, "title", link)
            if any(ch["id"] == channel_id for ch in data.source_channels):
                await event.respond(f"⚠️ **{channel_name}** est déjà dans la liste.")
                return
            data.source_channels.append({"id": channel_id, "name": channel_name, "link": link})
            data.save()
            num = len(data.source_channels)
            await event.respond(
                f"✅ Source ajoutée (**n°{num}**) : **{channel_name}**\n\n"
                f"💡 Configure le routage avec `/setroute {num} <n° dest> [photo|video|tout]`"
            )
        except ValueError as e:
            await event.respond(f"❌ {e}")
        except Exception as e:
            await event.respond(f"❌ Erreur : {e}")

    # ── /removecanal ──────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/removecanal(@\w+)?\s+(\d+)$", incoming=True, from_users=OWN
    ))
    async def cmd_removecanal(event):
        num = int(event.pattern_match.group(2)) - 1
        if 0 <= num < len(data.source_channels):
            removed   = data.source_channels.pop(num)
            source_id = removed["id"]
            # Supprimer les règles liées à cette source
            before = len(data.routes)
            data.routes = [r for r in data.routes if r["source_id"] != source_id]
            removed_routes = before - len(data.routes)
            data.save()
            msg = f"🗑️ Source supprimée : **{removed['name']}**"
            if removed_routes:
                msg += f"\n⚠️ {removed_routes} règle(s) associée(s) supprimée(s)."
            await event.respond(msg)
        else:
            await event.respond("❌ Numéro invalide. Utilise `/canaux` pour voir la liste.")

    # ── /canaux ───────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/canaux(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_canaux(event):
        if not data.source_channels:
            await event.respond("📋 Aucun canal source.\nUtilise `/addcanal <lien>` pour en ajouter.")
            return
        lines = ["📋 **Canaux sources :**\n"]
        for i, ch in enumerate(data.source_channels, 1):
            lines.append(f"{i}. **{ch['name']}** (`{ch.get('link','?')}`)")
        await event.respond("\n".join(lines))

    # ── /adddestination ───────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/adddestination(@\w+)?\s+(.+)$", incoming=True, from_users=OWN
    ))
    async def cmd_adddestination(event):
        link = event.pattern_match.group(2).strip()
        try:
            entity       = await resolve_channel(link)
            channel_id   = entity.id
            channel_name = getattr(entity, "title", link)
            if any(d["id"] == channel_id for d in data.destination_channels):
                await event.respond(f"⚠️ **{channel_name}** est déjà dans les destinations.")
                return
            data.destination_channels.append({"id": channel_id, "name": channel_name, "link": link})
            _entity_cache[channel_id] = entity
            data.save()
            num = len(data.destination_channels)
            await event.respond(
                f"✅ Destination ajoutée (**n°{num}**) : **{channel_name}**\n\n"
                f"💡 Crée une règle avec `/setroute <n° source> {num} [photo|video|tout]`"
            )
            logger.info(f"Destination ajoutée: {channel_name} ({channel_id})")
        except ValueError as e:
            await event.respond(f"❌ {e}")
        except Exception as e:
            await event.respond(f"❌ Erreur : {e}")

    # ── /removedestination ────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/removedestination(@\w+)?\s+(\d+)$", incoming=True, from_users=OWN
    ))
    async def cmd_removedestination(event):
        num = int(event.pattern_match.group(2)) - 1
        if 0 <= num < len(data.destination_channels):
            removed = data.destination_channels.pop(num)
            dest_id = removed["id"]
            invalidate_entity_cache(dest_id)
            before = len(data.routes)
            data.routes = [r for r in data.routes if r["dest_id"] != dest_id]
            removed_routes = before - len(data.routes)
            data.save()
            msg = f"🗑️ Destination supprimée : **{removed['name']}**"
            if removed_routes:
                msg += f"\n⚠️ {removed_routes} règle(s) associée(s) supprimée(s)."
            await event.respond(msg)
        else:
            await event.respond("❌ Numéro invalide. Utilise `/destinations` pour voir la liste.")

    # ── /destinations ─────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/destinations(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_destinations(event):
        if not data.destination_channels:
            await event.respond(
                "📋 Aucune destination configurée.\n"
                "Utilise `/adddestination <lien>` pour en ajouter."
            )
            return
        lines = ["📬 **Canaux de réception :**\n"]
        for i, d_ in enumerate(data.destination_channels, 1):
            lines.append(f"{i}. **{d_['name']}** (`{d_.get('link','?')}`)")
        await event.respond("\n".join(lines))

    # ── /setroute ─────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/setroute(@\w+)?\s+(\d+)\s+(\d+)(?:\s+(photo|video|tout|all))?$",
        incoming=True, from_users=OWN
    ))
    async def cmd_setroute(event):
        src_num  = int(event.pattern_match.group(2)) - 1
        dest_num = int(event.pattern_match.group(3)) - 1
        filtre   = (event.pattern_match.group(4) or "tout").lower()
        if filtre in ("tout", "all"):
            filtre = "all"

        if src_num < 0 or src_num >= len(data.source_channels):
            await event.respond(
                f"❌ Source n°{src_num+1} inexistante. Utilise `/canaux` pour voir la liste."
            )
            return
        if dest_num < 0 or dest_num >= len(data.destination_channels):
            await event.respond(
                f"❌ Destination n°{dest_num+1} inexistante. Utilise `/destinations` pour voir la liste."
            )
            return

        src  = data.source_channels[src_num]
        dest = data.destination_channels[dest_num]

        # Vérifier si une règle identique existe déjà
        existing = next(
            (r for r in data.routes
             if r["source_id"] == src["id"] and r["dest_id"] == dest["id"]),
            None,
        )
        if existing:
            existing["filter"] = filtre
            action = "mise à jour"
        else:
            data.routes.append({"source_id": src["id"], "dest_id": dest["id"], "filter": filtre})
            action = "créée"

        data.save()

        filtre_label = {"all": "📷🎬 Tout", "photo": "📷 Photos uniquement", "video": "🎬 Vidéos uniquement"}[filtre]
        await event.respond(
            f"✅ Règle {action} !\n\n"
            f"📡 Source : **{src['name']}**\n"
            f"➡️ Destination : **{dest['name']}**\n"
            f"🔍 Filtre : {filtre_label}"
        )

    # ── /removeroute ──────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/removeroute(@\w+)?\s+(\d+)$", incoming=True, from_users=OWN
    ))
    async def cmd_removeroute(event):
        num = int(event.pattern_match.group(2)) - 1
        if 0 <= num < len(data.routes):
            route   = data.routes.pop(num)
            src_info  = next((c["name"] for c in data.source_channels  if c["id"] == route["source_id"]), str(route["source_id"]))
            dest_info = next((d["name"] for d in data.destination_channels if d["id"] == route["dest_id"]), str(route["dest_id"]))
            data.save()
            await event.respond(f"🗑️ Règle supprimée : **{src_info}** → **{dest_info}**")
        else:
            await event.respond("❌ Numéro invalide. Utilise `/routes` pour voir la liste.")

    # ── /routes ───────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/routes(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_routes(event):
        if not data.routes:
            no_route_msg = "📋 Aucune règle de routage.\n\nUtilise `/setroute <n° source> <n° dest> [photo|video|tout]`"
            if data.destination_channels:
                no_route_msg += (
                    f"\n\n💡 Sans règle, tous les médias vont vers la 1ère destination : "
                    f"**{data.destination_channels[0]['name']}**"
                )
            await event.respond(no_route_msg)
            return

        filtre_label = {"all": "📷🎬 Tout", "photo": "📷 Photos", "video": "🎬 Vidéos"}
        lines = ["🔀 **Règles de routage :**\n"]
        for i, route in enumerate(data.routes, 1):
            src_name  = next((c["name"] for c in data.source_channels  if c["id"] == route["source_id"]), f"id:{route['source_id']}")
            dest_name = next((d["name"] for d in data.destination_channels if d["id"] == route["dest_id"]), f"id:{route['dest_id']}")
            f_label   = filtre_label.get(route["filter"], route["filter"])
            lines.append(f"{i}. **{src_name}** → **{dest_name}** ({f_label})")

        if data.destination_channels:
            lines.append(
                f"\n💡 Sources sans règle → **{data.destination_channels[0]['name']}** (tout)"
            )
        await event.respond("\n".join(lines))

    # ── /setdestination (rétro-compatibilité) ─────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/setdestination(@\w+)?\s+(.+)$", incoming=True, from_users=OWN
    ))
    async def cmd_setdestination(event):
        link = event.pattern_match.group(2).strip()
        try:
            entity       = await resolve_channel(link)
            channel_name = getattr(entity, "title", link)
            channel_id   = entity.id
            # Ajouter si pas encore présent, sinon mettre à jour la 1ère
            if data.destination_channels:
                data.destination_channels[0] = {"id": channel_id, "name": channel_name, "link": link}
            else:
                data.destination_channels.append({"id": channel_id, "name": channel_name, "link": link})
            _entity_cache[channel_id] = entity
            data.save()
            await event.respond(
                f"✅ Destination par défaut : **{channel_name}**\n\n"
                f"💡 Tu peux ajouter d'autres destinations avec `/adddestination`"
            )
            logger.info(f"Destination définie: {channel_name} (id={entity.id})")
        except ValueError as e:
            await event.respond(f"❌ {e}")
        except Exception as e:
            await event.respond(f"❌ Erreur : {e}")

    # ── /pause ────────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/pause(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_pause(event):
        data.paused = True
        data.save()
        await event.respond("⏸️ **Mis en pause.** Les transferts sont suspendus.")

    # ── /resume ───────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/resume(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_resume(event):
        data.paused = False
        data.save()
        await event.respond("▶️ **Repris !** Les transferts recommencent.")

    # ── /clear ────────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/clear(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_clear(event):
        count = len(data.history_ids)
        data.history_ids.clear()
        data.save()
        await event.respond(f"🗑️ Historique effacé ({count} IDs supprimés).")

    # ── /status ───────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/status(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_status(event):
        state = "⏸️ En pause" if data.paused else "▶️ Actif"

        src_list = "\n".join(
            f"  {i}. {ch['name']}" for i, ch in enumerate(data.source_channels, 1)
        ) or "  Aucun"

        dest_list = "\n".join(
            f"  {i}. {d['name']}" for i, d in enumerate(data.destination_channels, 1)
        ) or "  Aucune"

        filtre_label = {"all": "Tout", "photo": "Photos", "video": "Vidéos"}
        routes_list  = "\n".join(
            f"  {i}. {next((c['name'] for c in data.source_channels if c['id']==r['source_id']), '?')} → "
            f"{next((d['name'] for d in data.destination_channels if d['id']==r['dest_id']), '?')} "
            f"({filtre_label.get(r['filter'], r['filter'])})"
            for i, r in enumerate(data.routes, 1)
        ) or "  Aucune (→ destination par défaut)"

        await event.respond(
            f"📊 **État du bot**\n\n"
            f"État : {state}\n\n"
            f"📡 **Sources ({len(data.source_channels)}) :**\n{src_list}\n\n"
            f"📬 **Destinations ({len(data.destination_channels)}) :**\n{dest_list}\n\n"
            f"🔀 **Règles ({len(data.routes)}) :**\n{routes_list}\n\n"
            f"🗂 IDs suivis : {len(data.history_ids)}"
        )

    # ── /stats ────────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/stats(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_stats(event):
        data.reset_stats_if_new_day()
        await event.respond(
            f"📈 **Statistiques**\n\n"
            f"Aujourd'hui : **{data.stats.get('today', 0)}** médias\n"
            f"Total : **{data.stats.get('total', 0)}** médias"
        )

    # ── /gethistory ───────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/gethistory(@\w+)?(?:\s+(.+))?$", incoming=True, from_users=OWN
    ))
    async def cmd_gethistory(event):
        if not data.destination_channels:
            await event.respond(
                "❌ Aucune destination configurée.\n"
                "Utilise `/adddestination <lien>` d'abord."
            )
            return

        link_arg    = event.pattern_match.group(2)
        target_link = (link_arg.strip() if link_arg else None) or (
            data.source_channels[0]["link"] if data.source_channels else None
        )
        if not target_link:
            await event.respond("❌ Spécifie un canal : `/gethistory @canal`")
            return

        status_msg = await event.respond("🔍 Connexion au canal en cours...")
        try:
            entity = await resolve_channel(target_link)
        except ValueError as e:
            await status_msg.edit(f"❌ {e}")
            return

        source_name = getattr(entity, "title", target_link)
        source_id   = entity.id

        # Trouver les règles pour cette source
        routes_for_source = data.get_routes_for_source(source_id)
        if routes_for_source:
            dest_names = ", ".join(
                next((d["name"] for d in data.destination_channels if d["id"] == r["dest_id"]), "?")
                for r in routes_for_source
            )
            route_info = f"🔀 Règles : {len(routes_for_source)} destination(s) ({dest_names})"
        else:
            default = data.get_default_dest()
            dest_names = default["name"] if default else "?"
            route_info = f"📬 Destination par défaut : {dest_names}"

        all_messages   = []
        offset_id      = 0
        total_fetched  = 0
        last_scan_update = 0.0

        await status_msg.edit(
            f"📡 **Scan de l'historique**\n"
            f"📺 Canal : **{source_name}**\n"
            f"{route_info}\n\n"
            f"⏳ Récupération des messages..."
        )

        while True:
            try:
                history = await user_client(
                    GetHistoryRequest(
                        peer=entity, limit=100, offset_date=None,
                        offset_id=offset_id, max_id=0, min_id=0,
                        add_offset=0, hash=0,
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
            offset_id      = history.messages[-1].id

            now_t = asyncio.get_event_loop().time()
            if now_t - last_scan_update >= 2.0:
                last_scan_update = now_t
                await status_msg.edit(
                    f"📡 **Scan de l'historique**\n"
                    f"📺 Canal : **{source_name}**\n"
                    f"{route_info}\n\n"
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
        skipped      = len(all_messages) - len(new_messages)
        total_new    = len(new_messages)

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
            f"📺 Canal : **{source_name}**\n"
            f"{route_info}\n\n"
            f"`{'░' * 16}` 0/{total_new}\n"
            f"✅ Envoyés : 0  ❌ Échecs : 0\n"
            f"⚡ Vitesse : calcul...\n"
            f"⏱ Temps restant : calcul..."
        )

        async def live_progress(s, f, total, speed, eta):
            bar        = _progress_bar(s + f, total)
            speed_str  = f"{speed:.1f} médias/min" if speed > 0 else "calcul..."
            await status_msg.edit(
                f"📤 **Envoi en cours**\n"
                f"📺 Canal : **{source_name}**\n"
                f"{route_info}\n\n"
                f"`{bar}` {s + f}/{total}\n"
                f"✅ Envoyés : **{s}**  ❌ Échecs : **{f}**\n"
                f"⚡ Vitesse : {speed_str}\n"
                f"⏱ Temps restant : ~{eta}"
            )

        sent, failed = await process_message_queue(
            new_messages, source_id, source_name,
            progress_callback=live_progress,
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


# ── Auto-sync GitHub ──────────────────────────────────────────────────────────

def auto_push_to_github():
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        return
    import base64, urllib.request, urllib.error
    owner, repo = "kns336cne", "bot-telegram-"
    filepath    = "telegram-bot/bot.py"
    url         = f"https://api.github.com/repos/{owner}/{repo}/contents/{filepath}"
    headers     = {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    }
    try:
        script_path = os.path.join(os.path.dirname(__file__), "bot.py")
        with open(script_path, "rb") as f:
            content_b64 = base64.b64encode(f.read()).decode()
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
        logger.info(f"GitHub auto-sync: {'mis à jour' if code == 200 else 'créé'} (HTTP {code})")
    except Exception as e:
        logger.warning(f"GitHub auto-sync échoué (non bloquant): {e}")


# ── Enregistrement du menu de commandes Telegram ──────────────────────────────

async def register_bot_commands():
    commands = [
        BotCommand(command="status",            description="État du bot"),
        BotCommand(command="canaux",            description="Canaux sources surveillés"),
        BotCommand(command="addcanal",          description="Ajouter un canal source"),
        BotCommand(command="removecanal",       description="Supprimer un canal source"),
        BotCommand(command="destinations",      description="Canaux de réception"),
        BotCommand(command="adddestination",    description="Ajouter un canal de réception"),
        BotCommand(command="removedestination", description="Supprimer une destination"),
        BotCommand(command="routes",            description="Règles de routage"),
        BotCommand(command="setroute",          description="Créer une règle source→dest+filtre"),
        BotCommand(command="removeroute",       description="Supprimer une règle"),
        BotCommand(command="gethistory",        description="Récupérer tout l'historique"),
        BotCommand(command="pause",             description="Mettre en pause"),
        BotCommand(command="resume",            description="Reprendre"),
        BotCommand(command="stats",             description="Statistiques"),
        BotCommand(command="clear",             description="Effacer l'historique des IDs"),
        BotCommand(command="help",              description="Aide complète"),
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


# ── Point d'entrée ────────────────────────────────────────────────────────────

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
        StringSession(SESSION_STRING), API_ID, API_HASH,
        connection_retries=-1, retry_delay=5,
    )
    bot_client = TelegramClient(
        StringSession(), API_ID, API_HASH,
        connection_retries=-1, retry_delay=5,
    )

    try:
        await user_client.connect()
        if not await user_client.is_user_authorized():
            raise RuntimeError(
                "SESSION_STRING invalide ou expiré. "
                "Régénère-le avec generate_session.py."
            )
        logger.info("Client utilisateur connecté avec SESSION_STRING")

        await bot_client.start(bot_token=BOT_TOKEN)
        logger.info("Client bot démarré")

        setup_user_handlers()
        setup_bot_handlers()
        await register_bot_commands()

        me     = await user_client.get_me()
        bot_me = await bot_client.get_me()
        logger.info(f"Client utilisateur: {me.first_name} (@{getattr(me, 'username', 'N/A')})")
        logger.info(f"Client bot: @{bot_me.username}")

        dest_summary = (
            ", ".join(d["name"] for d in data.destination_channels)
            if data.destination_channels else "Non configurée"
        )
        await notify_owner(
            f"🚀 **Bot démarré !**\n\n"
            f"👤 Compte : {me.first_name}\n"
            f"🤖 Bot : @{bot_me.username}\n"
            f"📡 Sources : {len(data.source_channels)}\n"
            f"📬 Destinations : {len(data.destination_channels)} ({dest_summary})\n"
            f"🔀 Règles : {len(data.routes)}"
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
