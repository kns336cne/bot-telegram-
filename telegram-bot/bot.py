"""
Telegram Bot - Transfert de médias entre canaux (y compris canaux restreints)
Utilise Telethon (session utilisateur) pour accéder aux canaux sans forwarding
et un client bot pour les commandes.

Version améliorée : écriture JSON atomique, téléchargement sur disque,
gestion d'erreurs plus sûre, multi-destinations, filtres de médias par canal.
"""

import asyncio
import json
import logging
import os
import tempfile
from datetime import datetime
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
DOWNLOAD_DIR = os.environ.get("DOWNLOAD_DIR", tempfile.gettempdir())

UPLOAD_SEMAPHORE = asyncio.Semaphore(5)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(8)

# Catégories de médias reconnues pour les filtres par canal
CATEGORIES = ["photo", "video", "gif", "file"]
CATEGORY_LABELS = {
    "photo": "📸 Photos",
    "video": "🎬 Vidéos",
    "gif": "🌀 GIFs",
    "file": "📄 Fichiers",
}

# Déduplication en mémoire : évite le double-envoi si l'event se déclenche 2x
_seen_messages: set[tuple[int, int]] = set()

# Cache entités destination (une par destination) — évite get_entity() à chaque message
_dest_entity_cache: dict[int, object] = {}

# Buffer albums : grouped_id → [(message, source_name, chat_id, dest_id)]
_album_buffer: dict[int, list] = {}
_album_flush_tasks: dict[int, asyncio.Task] = {}
ALBUM_WAIT = 0.8  # secondes pour collecter tous les médias d'un album

user_client: TelegramClient = None
bot_client: TelegramClient = None
_start_time: datetime | None = None
_me_info: dict = {}  # infos du compte utilisateur connecté (rempli au démarrage)
_bot_me_info: dict = {}  # infos du bot (rempli au démarrage)


class BotData:
    def __init__(self):
        self.source_channels: list[dict] = []
        self.destinations: list[dict] = []  # [{"id": channel_id, "name":.., "link":..}]
        self.paused: bool = False
        self.dedupe_enabled: bool = True  # si False, renvoie tout même déjà transféré
        self.stats: dict = {"today": 0, "total": 0, "date": str(datetime.now().date())}
        self.channel_stats: dict[str, int] = {}  # str(channel_id) → total envoyé
        self.history_ids: set[int] = set()
        self.invite_cache: dict[str, int] = {}  # invite_hash → channel_id
        self._load()

    def _load(self):
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, "r") as f:
                    raw = json.load(f)
                self.source_channels = raw.get("source_channels", [])
                self.destinations = raw.get("destinations", [])
                self.paused = raw.get("paused", False)
                self.dedupe_enabled = raw.get("dedupe_enabled", True)
                self.stats = raw.get("stats", self.stats)
                self.channel_stats = raw.get("channel_stats", {})
                self.history_ids = set(raw.get("history_ids", []))
                self.invite_cache = raw.get("invite_cache", {})

                # Migration depuis l'ancien format à destination unique
                if not self.destinations and raw.get("destination_id"):
                    self.destinations = [{
                        "id": raw["destination_id"],
                        "name": raw.get("destination") or "Destination",
                        "link": raw.get("destination") or "",
                    }]
                    logger.info("Migration: ancienne destination unique convertie en liste.")

                # S'assure que chaque canal source a bien les clés attendues
                for ch in self.source_channels:
                    ch.setdefault("dest_index", None)
                    ch.setdefault("filters", list(CATEGORIES))

                logger.info(
                    f"Données chargées: {len(self.source_channels)} canaux source, "
                    f"{len(self.destinations)} destination(s)"
                )
            except Exception as e:
                logger.error(f"Erreur chargement données: {e}")

    def save(self):
        """Écriture atomique : écrit dans un fichier temporaire puis remplace
        l'ancien via os.replace(), qui est atomique sur la plupart des OS.
        Évite de corrompre bot_data.json si le process est tué en plein write."""
        try:
            dir_name = os.path.dirname(os.path.abspath(DATA_FILE)) or "."
            fd, tmp_path = tempfile.mkstemp(dir=dir_name, prefix=".bot_data_", suffix=".tmp")
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(
                        {
                            "source_channels": self.source_channels,
                            "destinations": self.destinations,
                            "paused": self.paused,
                            "dedupe_enabled": self.dedupe_enabled,
                            "stats": self.stats,
                            "channel_stats": self.channel_stats,
                            "history_ids": list(self.history_ids),
                            "invite_cache": self.invite_cache,
                        },
                        f,
                        indent=2,
                    )
                os.replace(tmp_path, DATA_FILE)
            except Exception:
                if os.path.exists(tmp_path):
                    os.remove(tmp_path)
                raise
        except Exception as e:
            logger.error(f"Erreur sauvegarde: {e}")

    def reset_stats_if_new_day(self):
        today = str(datetime.now().date())
        if self.stats.get("date") != today:
            self.stats["today"] = 0
            self.stats["date"] = today
            self.save()

    def increment_stats(self, count=1, channel_id: int | None = None):
        self.reset_stats_if_new_day()
        self.stats["today"] = self.stats.get("today", 0) + count
        self.stats["total"] = self.stats.get("total", 0) + count
        if channel_id is not None:
            key = str(channel_id)
            self.channel_stats[key] = self.channel_stats.get(key, 0) + count
        self.save()

    def get_channel_destination(self, channel: dict) -> dict | None:
        """Retourne la destination (dict) vers laquelle ce canal doit envoyer,
        ou None si aucune destination n'est configurée du tout."""
        idx = channel.get("dest_index")
        if idx and 1 <= idx <= len(self.destinations):
            return self.destinations[idx - 1]
        if self.destinations:
            return self.destinations[0]
        return None

    def get_channel_filters(self, channel: dict) -> list[str]:
        filters = channel.get("filters")
        if not filters:
            return list(CATEGORIES)
        return filters


data = BotData()

# ── Helpers globaux ───────────────────────────────────────────────────────────

_EXT_MAP = {
    "video/mp4": "mp4", "video/quicktime": "mov",
    "video/x-matroska": "mkv", "video/webm": "webm",
    "video/avi": "avi", "video/3gpp": "3gp",
    "image/jpeg": "jpg", "image/png": "png",
    "image/gif": "gif", "image/webp": "webp",
}


def _media_kind(message) -> tuple[str, str, bool]:
    """Détermine (nom_fichier, categorie, force_document) pour un message média,
    sans avoir besoin des bytes en mémoire.
    categorie ∈ {"photo", "video", "gif", "file", "unknown"} — utilisée pour les filtres."""
    if isinstance(message.media, MessageMediaPhoto):
        return "photo.jpg", "photo", False

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
            return f"video.{ext}", "video", False
        elif is_gif:
            return "animation.gif", "gif", False
        elif is_image:
            ext = _EXT_MAP.get(mime, "jpg")
            return f"photo.{ext}", "photo", False
        else:
            raw_ext = mime.split("/")[-1] if "/" in mime else "bin"
            return f"file.{raw_ext}", "file", True

    return "", "unknown", False


async def get_dest_entity(dest_id: int):
    """Cache l'entité destination par id — évite get_entity() à chaque message."""
    if dest_id in _dest_entity_cache:
        return _dest_entity_cache[dest_id]
    try:
        entity = await user_client.get_entity(PeerChannel(dest_id))
        _dest_entity_cache[dest_id] = entity
        return entity
    except Exception as e:
        logger.error(f"Impossible de récupérer la destination (id={dest_id}): {e}")
        return None


def invalidate_dest_cache(dest_id: int | None = None):
    if dest_id is None:
        _dest_entity_cache.clear()
    else:
        _dest_entity_cache.pop(dest_id, None)


async def resolve_channel(identifier: str):
    """
    Résout un canal depuis un lien public, username ou lien d'invitation privé.
    Pour les liens privés (+HASH), évite CheckChatInviteRequest (FloodWait fréquent)
    en cherchant d'abord dans les canaux déjà connus, puis via ImportChatInviteRequest.
    """
    identifier = identifier.strip()

    if "t.me/" in identifier:
        identifier = identifier.split("t.me/")[-1].strip("/")

    if identifier.startswith("+"):
        invite_hash = identifier[1:]

        def _extract_hash(link: str) -> str:
            if "+" in link:
                return link.split("+")[-1].strip("/")
            return ""

        if invite_hash in data.invite_cache:
            try:
                return await user_client.get_entity(PeerChannel(data.invite_cache[invite_hash]))
            except Exception:
                del data.invite_cache[invite_hash]
                data.save()

        known_links = [(ch.get("link", ""), ch["id"]) for ch in data.source_channels]
        for d in data.destinations:
            known_links.append((d.get("link", ""), d["id"]))

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
            logger.error(f"resolve_channel(+{invite_hash}) a échoué: {e}")
            raise ValueError("Impossible de rejoindre ce canal. Vérifie le lien.")

    if not identifier.startswith("@"):
        identifier = "@" + identifier

    try:
        return await user_client.get_entity(identifier)
    except Exception:
        try:
            return await user_client.get_entity(identifier.lstrip("@"))
        except Exception as e:
            logger.error(f"resolve_channel({identifier}) a échoué: {e}")
            raise ValueError(f"Impossible de résoudre le canal `{identifier}`.")


async def _download_to_disk(message) -> str | None:
    """Télécharge un média directement sur disque (jamais en RAM).
    Retourne le chemin du fichier temporaire, ou None en cas d'échec."""
    try:
        async with DOWNLOAD_SEMAPHORE:
            path = await user_client.download_media(message, file=DOWNLOAD_DIR)
        return path
    except Exception as e:
        logger.warning(f"Échec téléchargement msg_id={getattr(message, 'id', '?')}: {e}")
        return None


def _rename_for_upload(path: str, message) -> str:
    """Renomme le fichier téléchargé pour que Telegram détecte bien le type
    (photo/vidéo/gif) à partir de l'extension, sans recharger le contenu en RAM."""
    filename, category, _ = _media_kind(message)
    if not filename:
        return path
    new_path = os.path.join(os.path.dirname(path), filename)
    if new_path != path:
        try:
            os.replace(path, new_path)
            return new_path
        except Exception:
            return path
    return path


async def send_media_to_destination(message, dest_id: int, caption_override: str = None) -> bool:
    """Télécharge sur disque et renvoie un média vers la destination donnée."""
    dest_entity = await get_dest_entity(dest_id)
    if not dest_entity:
        logger.warning(f"Destination {dest_id} introuvable")
        return False

    caption = caption_override if caption_override is not None else (message.text or "")
    path = None

    try:
        path = await _download_to_disk(message)
        if path is None:
            return False

        filename, category, force_doc = _media_kind(message)
        if category == "unknown":
            return False

        path = _rename_for_upload(path, message)
        extra_kwargs = {"force_document": force_doc}
        if category == "video":
            extra_kwargs["supports_streaming"] = True

        kwargs = dict(file=path, caption=caption, **extra_kwargs)

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
    finally:
        if path and os.path.exists(path):
            try:
                os.remove(path)
            except Exception as e:
                logger.warning(f"Impossible de supprimer le fichier temporaire {path}: {e}")


async def send_album_to_destination(messages: list, source_name: str, dest_id: int) -> int:
    """Télécharge tous les médias en parallèle (sur disque) et les envoie en un album groupé."""
    dest_entity = await get_dest_entity(dest_id)
    if not dest_entity:
        return 0

    downloaded = await asyncio.gather(*[_download_to_disk(m) for m in messages])

    files = []
    for msg, path in zip(messages, downloaded):
        if path is None:
            continue
        path = _rename_for_upload(path, msg)
        files.append(path)

    if not files:
        return 0

    caption = f"📺 Source: {source_name}\n📅 {now_paris().strftime('%d/%m/%Y à %H:%M')}"
    captions = [caption] + [""] * (len(files) - 1)
    send_kwargs = dict(file=files, caption=captions)

    try:
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
                for msg in messages:
                    ok = await send_media_to_destination(msg, dest_id)
                    if ok:
                        count += 1
                return count
    finally:
        for path in files:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception as e:
                    logger.warning(f"Impossible de supprimer {path}: {e}")


async def flush_album(grouped_id: int):
    """Attend ALBUM_WAIT s pour collecter tous les médias du pack, puis envoie d'un coup."""
    try:
        await asyncio.sleep(ALBUM_WAIT)
        items = _album_buffer.pop(grouped_id, [])
        if not items:
            return

        items.sort(key=lambda x: x[0].id)
        source_name = items[0][1]
        source_chat_id = items[0][2]
        dest_id = items[0][3]

        new_msgs = []
        for msg, _sname, cid, _did in items:
            msg_key = (cid, msg.id)
            if msg_key in _seen_messages:
                continue
            _seen_messages.add(msg_key)
            if data.dedupe_enabled and msg.id in data.history_ids:
                continue
            data.history_ids.add(msg.id)
            new_msgs.append(msg)

        if not new_msgs:
            return

        data.save()
        logger.info(f"Album groupé {grouped_id}: {len(new_msgs)} médias depuis {source_name}")

        count = await send_album_to_destination(new_msgs, source_name, dest_id)
        if count > 0:
            data.increment_stats(count, channel_id=source_chat_id)
            await notify_owner(
                f"✅ Album transféré ! ({count} médias)\n"
                f"📚 Depuis **{source_name}**\n"
                f"⏰ {now_paris().strftime('%d/%m/%Y à %H:%M')}"
            )
    except Exception as e:
        logger.error(f"Erreur flush_album({grouped_id}): {e}")
    finally:
        _album_flush_tasks.pop(grouped_id, None)


def _progress_bar(done: int, total: int, width: int = 16) -> str:
    """Génère une barre de progression style ████░░░░"""
    if total == 0:
        return "░" * width
    filled = int(width * done / total)
    return "█" * filled + "░" * (width - filled)


async def process_message_queue(
    messages: list,
    dest_id: int,
    source_name: str = "",
    progress_callback=None,
    source_channel_id: int | None = None,
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
        result = await send_media_to_destination(msg, dest_id)
        if result is True:
            sent += 1
        else:
            failed += 1
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

    if progress_callback:
        await progress_callback(sent, failed, total, 0, "0s")

    data.increment_stats(sent, channel_id=source_channel_id)
    logger.info(f"Lot terminé: {sent} envoyés, {failed} échecs")
    return sent, failed


async def notify_owner(text: str):
    try:
        await bot_client.send_message(OWNER_ID, text, parse_mode="md")
    except Exception as e:
        logger.error(f"Impossible de notifier le propriétaire: {e}")


def setup_user_handlers():
    """Surveille les nouveaux messages dans les canaux sources."""

    @user_client.on(events.NewMessage())
    async def on_new_message(event):
        if data.paused or not data.source_channels or not data.destinations:
            return

        try:
            chat = await event.get_chat()
            chat_id = getattr(chat, "id", None)
            if chat_id is None:
                return

            channel = next((ch for ch in data.source_channels if ch["id"] == chat_id), None)
            if channel is None:
                return

            msg = event.message
            if not msg.media or not isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
                return

            # ── Filtre par type de média ────────────────────────────────
            _, category, _ = _media_kind(msg)
            if category == "unknown" or category not in data.get_channel_filters(channel):
                return

            dest = data.get_channel_destination(channel)
            if dest is None:
                return
            dest_id = dest["id"]
            source_name = channel["name"]

            if msg.grouped_id:
                gid = msg.grouped_id
                if gid not in _album_buffer:
                    _album_buffer[gid] = []
                _album_buffer[gid].append((msg, source_name, chat_id, dest_id))
                if gid in _album_flush_tasks and not _album_flush_tasks[gid].done():
                    _album_flush_tasks[gid].cancel()
                _album_flush_tasks[gid] = asyncio.create_task(flush_album(gid))
                return

            # _seen_messages protège toujours contre un double déclenchement du
            # même event (pas lié au réglage /doublons, c'est juste anti-doublon
            # technique). Le suivi history_ids, lui, respecte /doublons.
            msg_key = (chat_id, msg.id)
            if msg_key in _seen_messages:
                return
            _seen_messages.add(msg_key)

            if data.dedupe_enabled and msg.id in data.history_ids:
                return
            data.history_ids.add(msg.id)
            data.save()

            caption_parts = []
            if msg.text:
                caption_parts.append(msg.text)
            caption_parts.append(f"\n📺 Source: {source_name}")
            caption_parts.append(f"📅 {now_paris().strftime('%d/%m/%Y à %H:%M')}")
            caption = "\n".join(caption_parts)

            success = await send_media_to_destination(msg, dest_id, caption)
            media_type = CATEGORY_LABELS.get(category, category)

            if success:
                data.increment_stats(1, channel_id=chat_id)
                logger.info(f"Média transféré depuis {source_name} (msg_id={msg.id})")
                await notify_owner(
                    f"✅ Transfert réussi !\n"
                    f"{media_type} de **{source_name}**\n"
                    f"➡️ {dest['name']}\n"
                    f"⏰ {now_paris().strftime('%d/%m/%Y à %H:%M')}"
                )
            else:
                logger.warning(f"Échec transfert depuis {source_name} (msg_id={msg.id})")

        except Exception as e:
            logger.error(f"Erreur handler new message: {e}")


def _parse_index(text: str, max_len: int) -> int | None:
    """Parse un numéro 1-based fourni par l'utilisateur, retourne l'index 1-based
    valide ou None."""
    try:
        n = int(text)
    except ValueError:
        return None
    if 1 <= n <= max_len:
        return n
    return None


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
        pattern=r"^/whoami(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_whoami(event):
        user_str = (
            f"{_me_info.get('first_name', '?')} "
            f"(@{_me_info.get('username') or 'N/A'}, id={_me_info.get('id', '?')})"
        )
        bot_str = f"@{_bot_me_info.get('username', '?')} (id={_bot_me_info.get('id', '?')})"
        await event.respond(
            f"🪪 **Identité du bot**\n\n"
            f"👤 Compte utilisateur : {user_str}\n"
            f"🤖 Bot : {bot_str}\n"
            f"🔑 OWNER_ID configuré : `{OWNER_ID}`\n"
            f"📨 Cette commande vient de : `{event.sender_id}`"
        )

    @bot_client.on(events.NewMessage(
        pattern=r"^/ping(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_ping(event):
        t0 = asyncio.get_event_loop().time()
        msg = await event.respond("🏓 Ping...")
        rtt_ms = int((asyncio.get_event_loop().time() - t0) * 1000)
        uptime_str = "inconnu"
        if _start_time:
            delta = now_paris() - _start_time
            h, rem = divmod(int(delta.total_seconds()), 3600)
            m, s = divmod(rem, 60)
            uptime_str = f"{h}h {m}m {s}s"
        user_ok = "✅" if user_client and user_client.is_connected() else "❌"
        bot_ok = "✅" if bot_client and bot_client.is_connected() else "❌"
        await msg.edit(
            f"🏓 **Pong !** ({rtt_ms} ms)\n\n"
            f"Client utilisateur : {user_ok}\n"
            f"Client bot : {bot_ok}\n"
            f"⏱ En ligne depuis : {uptime_str}"
        )

    @bot_client.on(events.NewMessage(
        pattern=r"^/statscanal(@\w+)?\s+(\d+)$", incoming=True, from_users=OWN
    ))
    async def cmd_statscanal(event):
        idx = _parse_index(event.pattern_match.group(2), len(data.source_channels))
        if idx is None:
            await event.respond("❌ Numéro invalide. Utilise `/canaux` pour voir la liste.")
            return
        channel = data.source_channels[idx - 1]
        count = data.channel_stats.get(str(channel["id"]), 0)
        dest = data.get_channel_destination(channel)
        await event.respond(
            f"📈 **Statistiques — {channel['name']}**\n\n"
            f"Médias transférés : **{count}**\n"
            f"➡️ Destination : {dest['name'] if dest else 'aucune'}"
        )

    @bot_client.on(events.NewMessage(
        pattern=r"^/testdestination(@\w+)?\s+(\d+)$", incoming=True, from_users=OWN
    ))
    async def cmd_testdestination(event):
        idx = _parse_index(event.pattern_match.group(2), len(data.destinations))
        if idx is None:
            await event.respond("❌ Numéro invalide. Utilise `/destinations` pour voir la liste.")
            return
        dest = data.destinations[idx - 1]
        entity = await get_dest_entity(dest["id"])
        if not entity:
            await event.respond(f"❌ Impossible de joindre **{dest['name']}** — vérifie que le compte y a toujours accès.")
            return
        try:
            await user_client.send_message(
                entity,
                f"✅ Test de connexion depuis le bot — {now_paris().strftime('%d/%m/%Y à %H:%M')}",
            )
            await event.respond(f"✅ Message de test envoyé vers **{dest['name']}**.")
        except Exception as e:
            logger.error(f"cmd_testdestination({dest['name']}) a échoué: {e}")
            await event.respond(f"❌ Échec de l'envoi vers **{dest['name']}**. Vérifie les droits du compte sur ce canal.")

    @bot_client.on(events.NewMessage(
        pattern=r"^/backup(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_backup(event):
        if not os.path.exists(DATA_FILE):
            await event.respond("❌ Aucune donnée à sauvegarder pour le moment.")
            return
        await event.respond(
            file=DATA_FILE,
            message=f"💾 Sauvegarde de la configuration — {now_paris().strftime('%d/%m/%Y à %H:%M')}",
        )

    @bot_client.on(events.NewMessage(
        pattern=r"^/help(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_help(event):
        await event.respond(
            "🤖 **Commandes disponibles**\n\n"
            "**📡 Canaux source**\n"
            "`/addcanal` `<lien>` — ajouter un canal source\n"
            "`/removecanal` `<n°>` — supprimer un canal source\n"
            "`/canaux` — voir les canaux surveillés (destination + filtres)\n\n"
            "**🎯 Destinations**\n"
            "`/adddestination` `<lien>` — ajouter une destination\n"
            "`/destinations` — lister les destinations\n"
            "`/removedestination` `<n°>` — supprimer une destination\n"
            "`/routecanal` `<n° canal> <n° destination>` — router un canal vers une destination\n\n"
            "**🎛️ Filtres de médias**\n"
            "`/filtrecanal` `<n° canal> <photo|video|gif|file|tous>` — définir les types autorisés\n"
            "(plusieurs types séparés par une virgule, ex: `photo,video`)\n\n"
            "**⚙️ Contrôle**\n"
            "`/pause` — mettre en pause\n"
            "`/resume` — reprendre\n"
            "`/clear` — effacer l'historique des IDs\n"
            "`/doublons` `on|off` — activer/désactiver la déduplication\n\n"
            "**📥 Historique**\n"
            "`/gethistory` `<lien>` — récupérer tout l'historique, du plus ancien "
            "au plus récent (respecte /doublons)\n\n"
            "**📊 Infos**\n"
            "`/status` — état du bot\n"
            "`/stats` — statistiques globales\n"
            "`/statscanal` `<n°>` — statistiques d'un canal précis\n"
            "`/whoami` — infos du compte connecté et OWNER_ID\n"
            "`/ping` — vérifie la connexion et l'uptime\n"
            "`/testdestination` `<n°>` — envoie un message test vers une destination\n"
            "`/backup` — reçois `bot_data.json` en fichier\n"
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
            data.source_channels.append({
                "id": channel_id,
                "name": channel_name,
                "link": link,
                "dest_index": None,
                "filters": list(CATEGORIES),
            })
            data.save()
            await event.respond(
                f"✅ Canal ajouté : **{channel_name}**\n"
                f"➡️ Destination : {data.destinations[0]['name'] if data.destinations else 'aucune — utilise /adddestination'}\n"
                f"🎛️ Filtres : tous les types"
            )
            logger.info(f"Canal source ajouté: {channel_name} ({channel_id})")
        except ValueError as e:
            await event.respond(f"❌ {e}")
        except Exception as e:
            logger.error(f"cmd_addcanal({link}) a échoué: {e}")
            await event.respond("❌ Une erreur est survenue. Vérifie le lien et réessaie.")

    @bot_client.on(events.NewMessage(
        pattern=r"^/removecanal(@\w+)?\s+(\d+)$", incoming=True, from_users=OWN
    ))
    async def cmd_removecanal(event):
        idx = _parse_index(event.pattern_match.group(2), len(data.source_channels))
        if idx is None:
            await event.respond("❌ Numéro invalide. Utilise `/canaux` pour voir la liste.")
            return
        removed = data.source_channels.pop(idx - 1)
        data.save()
        await event.respond(f"🗑️ Canal supprimé : **{removed['name']}**")

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
            dest = data.get_channel_destination(ch)
            dest_name = dest["name"] if dest else "⚠️ aucune destination"
            filters = data.get_channel_filters(ch)
            if len(filters) == len(CATEGORIES):
                filters_str = "tous"
            else:
                filters_str = ", ".join(CATEGORY_LABELS.get(f, f) for f in filters)
            lines.append(
                f"{i}. **{ch['name']}**\n"
                f"   ➡️ {dest_name}  |  🎛️ {filters_str}"
            )
        await event.respond("\n".join(lines))

    # ── Destinations ──────────────────────────────────────────────────────

    @bot_client.on(events.NewMessage(
        pattern=r"^/adddestination(@\w+)?\s+(.+)$", incoming=True, from_users=OWN
    ))
    async def cmd_adddestination(event):
        link = event.pattern_match.group(2).strip()
        try:
            entity = await resolve_channel(link)
            channel_name = getattr(entity, "title", link)
            if any(d["id"] == entity.id for d in data.destinations):
                await event.respond(f"⚠️ **{channel_name}** est déjà une destination.")
                return
            data.destinations.append({"id": entity.id, "name": channel_name, "link": link})
            data.save()
            is_first = len(data.destinations) == 1
            await event.respond(
                f"✅ Destination ajoutée : **{channel_name}**"
                + ("\n(utilisée par défaut pour les canaux sans routage spécifique)" if is_first else "")
            )
            logger.info(f"Destination ajoutée: {channel_name} (id={entity.id})")
        except ValueError as e:
            await event.respond(f"❌ {e}")
        except Exception as e:
            logger.error(f"cmd_adddestination({link}) a échoué: {e}")
            await event.respond("❌ Une erreur est survenue. Vérifie le lien et réessaie.")

    @bot_client.on(events.NewMessage(
        pattern=r"^/destinations(@\w+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_destinations(event):
        if not data.destinations:
            await event.respond(
                "📋 Aucune destination.\nUtilise `/adddestination <lien>` pour en ajouter."
            )
            return
        lines = ["🎯 **Destinations :**\n"]
        for i, d in enumerate(data.destinations, 1):
            tag = " (défaut)" if i == 1 else ""
            lines.append(f"{i}. **{d['name']}**{tag}")
        await event.respond("\n".join(lines))

    @bot_client.on(events.NewMessage(
        pattern=r"^/removedestination(@\w+)?\s+(\d+)$", incoming=True, from_users=OWN
    ))
    async def cmd_removedestination(event):
        idx = _parse_index(event.pattern_match.group(2), len(data.destinations))
        if idx is None:
            await event.respond("❌ Numéro invalide. Utilise `/destinations` pour voir la liste.")
            return
        removed = data.destinations.pop(idx - 1)
        invalidate_dest_cache(removed["id"])
        # Les canaux routés spécifiquement vers un index invalide retombent
        # automatiquement sur la destination par défaut (voir get_channel_destination).
        data.save()
        await event.respond(
            f"🗑️ Destination supprimée : **{removed['name']}**\n"
            f"Les canaux qui pointaient vers elle utilisent maintenant la destination par défaut."
        )

    @bot_client.on(events.NewMessage(
        pattern=r"^/routecanal(@\w+)?\s+(\d+)\s+(\d+)$", incoming=True, from_users=OWN
    ))
    async def cmd_routecanal(event):
        canal_idx = _parse_index(event.pattern_match.group(2), len(data.source_channels))
        dest_idx = _parse_index(event.pattern_match.group(3), len(data.destinations))
        if canal_idx is None:
            await event.respond("❌ Numéro de canal invalide. Utilise `/canaux` pour voir la liste.")
            return
        if dest_idx is None:
            await event.respond("❌ Numéro de destination invalide. Utilise `/destinations` pour voir la liste.")
            return
        channel = data.source_channels[canal_idx - 1]
        channel["dest_index"] = dest_idx
        data.save()
        await event.respond(
            f"✅ **{channel['name']}** est maintenant routé vers "
            f"**{data.destinations[dest_idx - 1]['name']}**"
        )

    # ── Filtres ───────────────────────────────────────────────────────────

    @bot_client.on(events.NewMessage(
        pattern=r"^/filtrecanal(@\w+)?\s+(\d+)\s+(\S+)$", incoming=True, from_users=OWN
    ))
    async def cmd_filtrecanal(event):
        canal_idx = _parse_index(event.pattern_match.group(2), len(data.source_channels))
        if canal_idx is None:
            await event.respond("❌ Numéro de canal invalide. Utilise `/canaux` pour voir la liste.")
            return

        raw_filters = event.pattern_match.group(3).strip().lower()
        if raw_filters == "tous":
            new_filters = list(CATEGORIES)
        else:
            requested = [f.strip() for f in raw_filters.split(",") if f.strip()]
            invalid = [f for f in requested if f not in CATEGORIES]
            if invalid or not requested:
                await event.respond(
                    f"❌ Type(s) invalide(s) : {', '.join(invalid) if invalid else raw_filters}\n"
                    f"Valeurs possibles : `photo`, `video`, `gif`, `file`, ou `tous`."
                )
                return
            new_filters = requested

        channel = data.source_channels[canal_idx - 1]
        channel["filters"] = new_filters
        data.save()
        filters_str = "tous" if len(new_filters) == len(CATEGORIES) else ", ".join(
            CATEGORY_LABELS.get(f, f) for f in new_filters
        )
        await event.respond(f"✅ Filtre de **{channel['name']}** mis à jour : {filters_str}")

    @bot_client.on(events.NewMessage(
        pattern=r"^/doublons(@\w+)?(?:\s+(on|off))?$", incoming=True, from_users=OWN
    ))
    async def cmd_doublons(event):
        choice = event.pattern_match.group(2)
        if choice is None:
            state = "✅ activé (les médias déjà transférés sont ignorés)" if data.dedupe_enabled \
                else "❌ désactivé (tout est renvoyé, même déjà transféré)"
            await event.respond(f"🔁 **Déduplication** : {state}\n\nUtilise `/doublons on` ou `/doublons off`.")
            return
        data.dedupe_enabled = (choice == "on")
        data.save()
        if data.dedupe_enabled:
            await event.respond("✅ Déduplication **activée** — les médias déjà envoyés seront ignorés.")
        else:
            await event.respond(
                "⚠️ Déduplication **désactivée** — `/gethistory` et la surveillance en direct "
                "vont renvoyer TOUS les médias, y compris ceux déjà transférés."
            )

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
        dedupe_state = "✅ activée" if data.dedupe_enabled else "❌ désactivée"
        dest_list = "\n".join(
            f"  • {d['name']}" for d in data.destinations
        ) or "  Aucune — utilise /adddestination"
        src_list = "\n".join(
            f"  • {ch['name']}" for ch in data.source_channels
        ) or "  Aucun"
        await event.respond(
            f"📊 **État du bot**\n\n"
            f"État : {state}\n"
            f"Déduplication : {dedupe_state}\n"
            f"Destinations ({len(data.destinations)}) :\n{dest_list}\n\n"
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
        if not data.destinations:
            await event.respond(
                "❌ Aucune destination configurée.\n"
                "Utilise `/adddestination <lien>` d'abord."
            )
            return

        link_arg = event.pattern_match.group(2)
        target_link = (link_arg.strip() if link_arg else None) or (
            data.source_channels[0]["link"] if data.source_channels else None
        )
        if not target_link:
            await event.respond("❌ Spécifie un canal : `/gethistory @canal`")
            return

        # Destination : celle du canal si déjà connu, sinon la destination par défaut
        known_channel = next(
            (ch for ch in data.source_channels if ch.get("link") == target_link), None
        )
        dest = data.get_channel_destination(known_channel) if known_channel else data.destinations[0]
        dest_id = dest["id"]

        status_msg = await event.respond("🔍 Connexion au canal en cours...")

        try:
            entity = await resolve_channel(target_link)
        except ValueError as e:
            await status_msg.edit(f"❌ {e}")
            return

        source_name = getattr(entity, "title", target_link)
        allowed_filters = data.get_channel_filters(known_channel) if known_channel else list(CATEGORIES)
        all_messages = []
        offset_id = 0
        total_fetched = 0
        last_scan_update = 0.0

        await status_msg.edit(
            f"📡 **Scan de l'historique**\n"
            f"📺 Canal : **{source_name}**\n"
            f"➡️ Destination : **{dest['name']}**\n\n"
            f"⏳ Récupération des messages..."
        )

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
                logger.error(f"cmd_gethistory: erreur récupération historique: {e}")
                await status_msg.edit("❌ Erreur lors de la récupération de l'historique.")
                return

            if not history.messages:
                break

            for msg in history.messages:
                if msg.media and isinstance(msg.media, (MessageMediaPhoto, MessageMediaDocument)):
                    _, cat, _ = _media_kind(msg)
                    if cat in allowed_filters:
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

        # Tri chronologique : la toute première vidéo/photo postée dans le canal
        # part en premier, jusqu'à la plus récente.
        all_messages.sort(key=lambda m: m.id)

        if data.dedupe_enabled:
            new_messages = [m for m in all_messages if m.id not in data.history_ids]
        else:
            new_messages = all_messages  # /doublons off : renvoie tout, même déjà transféré
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
            new_messages, dest_id, source_name, progress_callback=live_progress,
            source_channel_id=entity.id,
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
    """
    Pousse bot.py vers GitHub automatiquement au démarrage si GITHUB_AUTO_PUSH=1
    ET GITHUB_TOKEN est défini. Désactivé par défaut : à activer explicitement
    pour éviter des pushes accidentels si plusieurs instances tournent en parallèle.
    """
    if os.environ.get("GITHUB_AUTO_PUSH", "0") != "1":
        return
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        logger.warning("GITHUB_AUTO_PUSH=1 mais GITHUB_TOKEN manquant, sync ignorée.")
        return

    import base64, urllib.request, urllib.error
    owner = os.environ.get("GITHUB_REPO_OWNER", "kns336cne")
    repo = os.environ.get("GITHUB_REPO_NAME", "bot-telegram-")
    filepath = os.environ.get("GITHUB_FILE_PATH", "telegram-bot/bot.py")
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
        BotCommand(command="status",            description="État du bot"),
        BotCommand(command="canaux",             description="Canaux surveillés"),
        BotCommand(command="addcanal",           description="Ajouter un canal source"),
        BotCommand(command="removecanal",        description="Supprimer un canal source"),
        BotCommand(command="destinations",       description="Lister les destinations"),
        BotCommand(command="adddestination",     description="Ajouter une destination"),
        BotCommand(command="removedestination",  description="Supprimer une destination"),
        BotCommand(command="routecanal",         description="Router un canal vers une destination"),
        BotCommand(command="filtrecanal",        description="Filtrer les types de médias d'un canal"),
        BotCommand(command="gethistory",         description="Récupérer tout l'historique"),
        BotCommand(command="pause",              description="Mettre en pause"),
        BotCommand(command="resume",             description="Reprendre"),
        BotCommand(command="doublons",           description="Activer/désactiver la déduplication"),
        BotCommand(command="stats",              description="Statistiques"),
        BotCommand(command="statscanal",         description="Statistiques d'un canal"),
        BotCommand(command="whoami",             description="Infos du compte connecté"),
        BotCommand(command="ping",               description="Vérifier la connexion"),
        BotCommand(command="testdestination",    description="Tester une destination"),
        BotCommand(command="backup",             description="Exporter la configuration"),
        BotCommand(command="clear",              description="Effacer l'historique des IDs"),
        BotCommand(command="help",               description="Aide"),
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

        me = await user_client.get_me()
        bot_me = await bot_client.get_me()
        logger.info(f"Client utilisateur: {me.first_name} (@{getattr(me, 'username', 'N/A')})")
        logger.info(f"Client bot: @{bot_me.username}")

        global _start_time
        _start_time = now_paris()
        _me_info.update({
            "id": me.id,
            "first_name": me.first_name,
            "username": getattr(me, "username", None),
        })
        _bot_me_info.update({
            "id": bot_me.id,
            "username": bot_me.username,
        })

        dest_summary = ", ".join(d["name"] for d in data.destinations) or "Non configurée"
        await notify_owner(
            f"🚀 **Bot démarré !**\n\n"
            f"👤 Compte : {me.first_name}\n"
            f"🤖 Bot : @{bot_me.username}\n"
            f"📡 Canaux surveillés : {len(data.source_channels)}\n"
            f"🎯 Destinations : {dest_summary}"
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
