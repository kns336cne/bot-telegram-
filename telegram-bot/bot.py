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
- Wizard interactif : commandes en étapes (style Discord)
- /cleardest : vider un canal de réception
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
from telethon.tl.functions.channels import DeleteMessagesRequest
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

API_ID         = int(os.environ["API_ID"])
API_HASH       = os.environ["API_HASH"]
SESSION_STRING = os.environ.get("SESSION_STRING", "")
BOT_TOKEN      = os.environ["BOT_TOKEN"]
OWNER_ID       = int(os.environ["OWNER_ID"])

DATA_FILE = "bot_data.json"

UPLOAD_SEMAPHORE   = asyncio.Semaphore(5)
DOWNLOAD_SEMAPHORE = asyncio.Semaphore(8)

_seen_messages: set[tuple[int, int]] = set()

_entity_cache: dict[int, object] = {}

_album_buffer: dict[int, list] = {}
_album_flush_tasks: dict[int, asyncio.Task] = {}
ALBUM_WAIT = 0.8

# ── État des wizards ──────────────────────────────────────────────────────────
# user_id → {"action": str, "step": int, "data": dict}
_user_states: dict[int, dict] = {}

user_client: TelegramClient = None
bot_client:  TelegramClient = None


# ── Données persistantes ──────────────────────────────────────────────────────

_GH_OWNER    = "kns336cne"
_GH_REPO     = "bot-telegram-"
_GH_DATA_PATH = "telegram-bot/bot_data.json"
_last_github_push: float = 0.0
_PUSH_INTERVAL = 30.0  # secondes minimum entre deux pushes GitHub


def _github_headers() -> dict:
    token = os.environ.get("GITHUB_TOKEN", "")
    return {
        "Authorization": f"token {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "Content-Type": "application/json",
    }


def _fetch_data_from_github() -> dict | None:
    """Télécharge bot_data.json depuis GitHub. Retourne le dict ou None."""
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        return None
    import base64, urllib.request, urllib.error
    url = f"https://api.github.com/repos/{_GH_OWNER}/{_GH_REPO}/contents/{_GH_DATA_PATH}"
    try:
        req = urllib.request.Request(url, headers=_github_headers())
        with urllib.request.urlopen(req) as r:
            resp = json.loads(r.read())
        content = base64.b64decode(resp["content"]).decode()
        data_dict = json.loads(content)
        logger.info("Données rechargées depuis GitHub (persistance entre redémarrages)")
        return data_dict
    except Exception as e:
        logger.info(f"Pas de données GitHub disponibles ({e}), démarrage à vide.")
        return None


def _push_data_to_github(payload_dict: dict):
    """Pousse bot_data.json vers GitHub (synchrone, non bloquant si erreur)."""
    global _last_github_push
    import time
    now = time.time()
    if now - _last_github_push < _PUSH_INTERVAL:
        return
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        return
    import base64, urllib.request, urllib.error
    url = f"https://api.github.com/repos/{_GH_OWNER}/{_GH_REPO}/contents/{_GH_DATA_PATH}"
    try:
        content_b64 = base64.b64encode(json.dumps(payload_dict, indent=2).encode()).decode()
        req = urllib.request.Request(url, headers=_github_headers())
        try:
            with urllib.request.urlopen(req) as r:
                existing = json.loads(r.read())
            sha = existing.get("sha")
        except urllib.error.HTTPError:
            sha = None
        body = {"message": "Auto-sync bot_data.json", "content": content_b64}
        if sha:
            body["sha"] = sha
        req2 = urllib.request.Request(
            url, data=json.dumps(body).encode(), headers=_github_headers(), method="PUT"
        )
        with urllib.request.urlopen(req2) as r:
            pass
        _last_github_push = now
        logger.info("bot_data.json synchronisé vers GitHub")
    except Exception as e:
        logger.warning(f"Sync GitHub bot_data.json échoué (non bloquant): {e}")


class BotData:
    def __init__(self):
        self.source_channels: list[dict] = []
        self.destination_channels: list[dict] = []
        self.destination: str | None = None
        self.destination_id: int | None = None
        self.routes: list[dict] = []
        self.paused: bool = False
        self.stats: dict = {"today": 0, "total": 0, "date": str(datetime.now().date())}
        self.history_ids: set[int] = set()
        self.invite_cache: dict[str, int] = {}
        self._load()

    def _load(self):
        # 1. Essai fichier local
        raw = None
        if os.path.exists(DATA_FILE):
            try:
                with open(DATA_FILE, "r") as f:
                    raw = json.load(f)
                logger.info("Données chargées depuis le fichier local.")
            except Exception as e:
                logger.error(f"Erreur lecture fichier local: {e}")

        # 2. Si pas de fichier local → essai GitHub
        if raw is None:
            raw = _fetch_data_from_github()
            if raw:
                # Sauvegarder localement pour les prochains accès
                try:
                    with open(DATA_FILE, "w") as f:
                        json.dump(raw, f, indent=2)
                except Exception:
                    pass

        if raw is None:
            logger.info("Aucune donnée trouvée — démarrage avec configuration vide.")
            return

        self.source_channels      = raw.get("source_channels", [])
        self.destination_channels = raw.get("destination_channels", [])
        self.destination          = raw.get("destination")
        self.destination_id       = raw.get("destination_id")
        self.routes               = raw.get("routes", [])
        self.paused               = raw.get("paused", False)
        self.stats                = raw.get("stats", self.stats)
        self.history_ids          = set(raw.get("history_ids", []))
        self.invite_cache         = raw.get("invite_cache", {})

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

    def _payload(self) -> dict:
        return {
            "source_channels":      self.source_channels,
            "destination_channels": self.destination_channels,
            "destination":          self.destination,
            "destination_id":       self.destination_id,
            "routes":               self.routes,
            "paused":               self.paused,
            "stats":                self.stats,
            "history_ids":          list(self.history_ids),
            "invite_cache":         self.invite_cache,
        }

    def save(self):
        if self.destination_channels:
            self.destination    = self.destination_channels[0].get("link")
            self.destination_id = self.destination_channels[0].get("id")
        payload = self._payload()
        # Sauvegarde locale
        try:
            with open(DATA_FILE, "w") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            logger.error(f"Erreur sauvegarde locale: {e}")
        # Sync GitHub (avec debounce)
        _push_data_to_github(payload)

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

    def get_routes_for_source(self, source_id: int) -> list[dict]:
        return [r for r in self.routes if r["source_id"] == source_id]

    def get_default_dest(self) -> dict | None:
        return self.destination_channels[0] if self.destination_channels else None

    def get_dest_by_id(self, dest_id: int) -> dict | None:
        return next((d for d in self.destination_channels if d["id"] == dest_id), None)


data = BotData()


# ── Map MIME ──────────────────────────────────────────────────────────────────

_EXT_MAP = {
    "video/mp4": "mp4",        "video/quicktime": "mov",
    "video/x-matroska": "mkv", "video/webm": "webm",
    "video/avi": "avi",        "video/3gpp": "3gp",
    "image/jpeg": "jpg",       "image/png": "png",
    "image/gif": "gif",        "image/webp": "webp",
}


# ── Helpers médias ────────────────────────────────────────────────────────────

def _get_media_kind(message) -> str:
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
    if route_filter == "all":
        return True
    if route_filter == "photo":
        return media_kind in ("photo", "image", "gif")
    if route_filter == "video":
        return media_kind == "video"
    return True


def _media_to_file(message, media_bytes: bytes):
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


# ── Wizard : gestion des états ────────────────────────────────────────────────

def state_set(user_id: int, action: str, step: int = 1, data_: dict = None):
    _user_states[user_id] = {"action": action, "step": step, "data": data_ or {}}

def state_get(user_id: int) -> dict | None:
    return _user_states.get(user_id)

def state_clear(user_id: int):
    _user_states.pop(user_id, None)

def state_next(user_id: int, extra: dict = None):
    s = _user_states.get(user_id)
    if s:
        s["step"] += 1
        if extra:
            s["data"].update(extra)


# ── Résolution de canal ───────────────────────────────────────────────────────

async def resolve_channel(identifier: str):
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


# ── Envoi d'un média ──────────────────────────────────────────────────────────

async def send_media_to_destination(
    message,
    dest_id: int,
    caption_override: str = None,
) -> bool:
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


# ── Envoi d'un album ──────────────────────────────────────────────────────────

async def send_album_to_destination(
    messages: list,
    source_name: str,
    dest_id: int,
    media_filter: str = "all",
) -> int:
    dest_entity = await get_entity_cached(dest_id)
    if not dest_entity:
        return 0

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


# ── Routage ───────────────────────────────────────────────────────────────────

async def route_and_send(message, source_id: int, source_name: str, caption: str) -> int:
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
        default = data.get_default_dest()
        if default:
            ok = await send_media_to_destination(message, default["id"], caption)
            if ok:
                sent += 1

    return sent


async def route_and_send_album(messages: list, source_id: int, source_name: str) -> int:
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


# ── File d'historique ─────────────────────────────────────────────────────────

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
    BATCH_SIZE  = 8
    sent        = 0
    failed      = 0
    last_update = 0.0

    media_messages = [
        m for m in messages
        if m.media and isinstance(m.media, (MessageMediaPhoto, MessageMediaDocument))
    ]
    total      = len(media_messages)
    start_time = asyncio.get_event_loop().time()
    logger.info(f"Traitement de {total} médias depuis {source_name}")

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


# ── Utilitaires ───────────────────────────────────────────────────────────────

def is_owner(sender_id: int) -> bool:
    return sender_id == OWNER_ID


async def notify_owner(text: str):
    try:
        await bot_client.send_message(OWNER_ID, text, parse_mode="md")
    except Exception as e:
        logger.error(f"Impossible de notifier le propriétaire: {e}")


def _fmt_sources() -> str:
    if not data.source_channels:
        return "_(aucune source configurée)_"
    return "\n".join(f"  **{i}.** {ch['name']}" for i, ch in enumerate(data.source_channels, 1))


def _fmt_destinations() -> str:
    if not data.destination_channels:
        return "_(aucune destination configurée)_"
    return "\n".join(f"  **{i}.** {d['name']}" for i, d in enumerate(data.destination_channels, 1))


def _fmt_routes() -> str:
    if not data.routes:
        return "_(aucune règle)_"
    fl = {"all": "Tout", "photo": "Photos", "video": "Vidéos"}
    lines = []
    for i, r in enumerate(data.routes, 1):
        s = next((c["name"] for c in data.source_channels if c["id"] == r["source_id"]), "?")
        d = next((x["name"] for x in data.destination_channels if x["id"] == r["dest_id"]), "?")
        lines.append(f"  **{i}.** {s} → {d} ({fl.get(r['filter'], r['filter'])})")
    return "\n".join(lines)


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

            if msg.grouped_id:
                gid = msg.grouped_id
                if gid not in _album_buffer:
                    _album_buffer[gid] = []
                _album_buffer[gid].append((msg, source_name, chat_id))
                if gid in _album_flush_tasks and not _album_flush_tasks[gid].done():
                    _album_flush_tasks[gid].cancel()
                _album_flush_tasks[gid] = asyncio.create_task(flush_album(gid))
                return

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

            count      = await route_and_send(msg, chat_id, source_name, caption)
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

    # ════════════════════════════════════════════════════════════════════════════
    # Wizard : handler catch-all — intercepte les réponses aux étapes
    # ════════════════════════════════════════════════════════════════════════════
    @bot_client.on(events.NewMessage(incoming=True, from_users=OWN))
    async def wizard_handler(event):
        text = (event.raw_text or "").strip()

        # Ignorer les commandes (traitées par leurs propres handlers)
        if text.startswith("/"):
            return

        state = state_get(OWNER_ID)
        if not state:
            return

        action = state["action"]
        step   = state["step"]
        sdata  = state["data"]

        # ── Annuler à tout moment ─────────────────────────────────────────────
        if text.lower() in ("annuler", "cancel", "stop"):
            state_clear(OWNER_ID)
            await event.respond("❌ Action annulée.")
            return

        try:

            # ──────────────────────────────────────────────────────────────────
            # AJOUTER SOURCE
            # ──────────────────────────────────────────────────────────────────
            if action == "add_canal":
                # step 1 : lien du canal
                try:
                    entity       = await resolve_channel(text)
                    channel_id   = entity.id
                    channel_name = getattr(entity, "title", text)
                except ValueError as e:
                    await event.respond(f"❌ {e}\n\n🔗 Renvoie le lien, ou tape `annuler`.")
                    return

                if any(ch["id"] == channel_id for ch in data.source_channels):
                    state_clear(OWNER_ID)
                    await event.respond(f"⚠️ **{channel_name}** est déjà dans la liste.")
                    return

                data.source_channels.append({"id": channel_id, "name": channel_name, "link": text})
                data.save()
                num = len(data.source_channels)
                state_clear(OWNER_ID)
                await event.respond(
                    f"✅ Source ajoutée (**n°{num}**) : **{channel_name}**\n\n"
                    f"💡 Configure le routage avec `/setroute`"
                )

            # ──────────────────────────────────────────────────────────────────
            # SUPPRIMER SOURCE
            # ──────────────────────────────────────────────────────────────────
            elif action == "remove_canal":
                try:
                    num = int(text) - 1
                except ValueError:
                    await event.respond("❌ Envoie un **numéro** valide, ou tape `annuler`.")
                    return

                if 0 <= num < len(data.source_channels):
                    removed   = data.source_channels.pop(num)
                    before    = len(data.routes)
                    data.routes = [r for r in data.routes if r["source_id"] != removed["id"]]
                    removed_routes = before - len(data.routes)
                    data.save()
                    state_clear(OWNER_ID)
                    msg = f"🗑️ Source supprimée : **{removed['name']}**"
                    if removed_routes:
                        msg += f"\n⚠️ {removed_routes} règle(s) associée(s) supprimée(s)."
                    await event.respond(msg)
                else:
                    await event.respond(
                        f"❌ Numéro invalide (1–{len(data.source_channels)}).\n"
                        f"Réessaie ou tape `annuler`."
                    )

            # ──────────────────────────────────────────────────────────────────
            # AJOUTER DESTINATION
            # ──────────────────────────────────────────────────────────────────
            elif action == "add_destination":
                try:
                    entity       = await resolve_channel(text)
                    channel_id   = entity.id
                    channel_name = getattr(entity, "title", text)
                except ValueError as e:
                    await event.respond(f"❌ {e}\n\n🔗 Renvoie le lien, ou tape `annuler`.")
                    return

                if any(d["id"] == channel_id for d in data.destination_channels):
                    state_clear(OWNER_ID)
                    await event.respond(f"⚠️ **{channel_name}** est déjà dans les destinations.")
                    return

                data.destination_channels.append({"id": channel_id, "name": channel_name, "link": text})
                _entity_cache[channel_id] = entity
                data.save()
                num = len(data.destination_channels)
                state_clear(OWNER_ID)
                await event.respond(
                    f"✅ Destination ajoutée (**n°{num}**) : **{channel_name}**\n\n"
                    f"💡 Crée une règle avec `/setroute`"
                )

            # ──────────────────────────────────────────────────────────────────
            # SUPPRIMER DESTINATION
            # ──────────────────────────────────────────────────────────────────
            elif action == "remove_destination":
                try:
                    num = int(text) - 1
                except ValueError:
                    await event.respond("❌ Envoie un **numéro** valide, ou tape `annuler`.")
                    return

                if 0 <= num < len(data.destination_channels):
                    removed = data.destination_channels.pop(num)
                    invalidate_entity_cache(removed["id"])
                    before = len(data.routes)
                    data.routes = [r for r in data.routes if r["dest_id"] != removed["id"]]
                    removed_routes = before - len(data.routes)
                    data.save()
                    state_clear(OWNER_ID)
                    msg = f"🗑️ Destination supprimée : **{removed['name']}**"
                    if removed_routes:
                        msg += f"\n⚠️ {removed_routes} règle(s) associée(s) supprimée(s)."
                    await event.respond(msg)
                else:
                    await event.respond(
                        f"❌ Numéro invalide (1–{len(data.destination_channels)}).\n"
                        f"Réessaie ou tape `annuler`."
                    )

            # ──────────────────────────────────────────────────────────────────
            # SETROUTE — étape 1 : numéro source
            # ──────────────────────────────────────────────────────────────────
            elif action == "set_route" and step == 1:
                try:
                    num = int(text) - 1
                except ValueError:
                    await event.respond("❌ Envoie un **numéro** valide, ou tape `annuler`.")
                    return

                if 0 <= num < len(data.source_channels):
                    src = data.source_channels[num]
                    state_next(OWNER_ID, {"src_num": num, "src": src})
                    await event.respond(
                        f"✅ Source : **{src['name']}**\n\n"
                        f"📬 **Destinations disponibles :**\n{_fmt_destinations()}\n\n"
                        f"👇 Envoie le **numéro** de la destination"
                    )
                else:
                    await event.respond(
                        f"❌ Numéro invalide (1–{len(data.source_channels)}).\n"
                        f"Réessaie ou tape `annuler`."
                    )

            # ──────────────────────────────────────────────────────────────────
            # SETROUTE — étape 2 : numéro destination
            # ──────────────────────────────────────────────────────────────────
            elif action == "set_route" and step == 2:
                try:
                    num = int(text) - 1
                except ValueError:
                    await event.respond("❌ Envoie un **numéro** valide, ou tape `annuler`.")
                    return

                if 0 <= num < len(data.destination_channels):
                    dest = data.destination_channels[num]
                    state_next(OWNER_ID, {"dest_num": num, "dest": dest})
                    await event.respond(
                        f"✅ Destination : **{dest['name']}**\n\n"
                        f"🔍 **Quel type de média envoyer ?**\n"
                        f"  📷  `photo` — photos uniquement\n"
                        f"  🎬  `video` — vidéos uniquement\n"
                        f"  ✨  `tout`  — photos + vidéos\n\n"
                        f"👇 Tape `photo`, `video` ou `tout`"
                    )
                else:
                    await event.respond(
                        f"❌ Numéro invalide (1–{len(data.destination_channels)}).\n"
                        f"Réessaie ou tape `annuler`."
                    )

            # ──────────────────────────────────────────────────────────────────
            # SETROUTE — étape 3 : filtre
            # ──────────────────────────────────────────────────────────────────
            elif action == "set_route" and step == 3:
                filtre = text.lower()
                if filtre in ("tout", "all"):
                    filtre = "all"
                if filtre not in ("photo", "video", "all"):
                    await event.respond(
                        "❌ Tape `photo`, `video` ou `tout`, ou tape `annuler`."
                    )
                    return

                src  = sdata["src"]
                dest = sdata["dest"]

                existing = next(
                    (r for r in data.routes
                     if r["source_id"] == src["id"] and r["dest_id"] == dest["id"]),
                    None,
                )
                if existing:
                    existing["filter"] = filtre
                    action_word = "mise à jour"
                else:
                    data.routes.append({"source_id": src["id"], "dest_id": dest["id"], "filter": filtre})
                    action_word = "créée"

                data.save()
                state_clear(OWNER_ID)
                fl = {"all": "📷🎬 Tout", "photo": "📷 Photos uniquement", "video": "🎬 Vidéos uniquement"}
                await event.respond(
                    f"✅ Règle {action_word} !\n\n"
                    f"📡 Source : **{src['name']}**\n"
                    f"➡️ Destination : **{dest['name']}**\n"
                    f"🔍 Filtre : {fl[filtre]}"
                )

            # ──────────────────────────────────────────────────────────────────
            # SUPPRIMER ROUTE
            # ──────────────────────────────────────────────────────────────────
            elif action == "remove_route":
                try:
                    num = int(text) - 1
                except ValueError:
                    await event.respond("❌ Envoie un **numéro** valide, ou tape `annuler`.")
                    return

                if 0 <= num < len(data.routes):
                    route     = data.routes.pop(num)
                    src_info  = next((c["name"] for c in data.source_channels if c["id"] == route["source_id"]), "?")
                    dest_info = next((d["name"] for d in data.destination_channels if d["id"] == route["dest_id"]), "?")
                    data.save()
                    state_clear(OWNER_ID)
                    await event.respond(f"🗑️ Règle supprimée : **{src_info}** → **{dest_info}**")
                else:
                    await event.respond(
                        f"❌ Numéro invalide (1–{len(data.routes)}).\n"
                        f"Réessaie ou tape `annuler`."
                    )

            # ──────────────────────────────────────────────────────────────────
            # GETHISTORY — étape 1 : lien du canal
            # ──────────────────────────────────────────────────────────────────
            elif action == "get_history":
                state_clear(OWNER_ID)
                await _do_gethistory(event, text)

            # ──────────────────────────────────────────────────────────────────
            # CLEARDEST — étape 1 : numéro de destination
            # ──────────────────────────────────────────────────────────────────
            elif action == "clear_dest" and step == 1:
                try:
                    num = int(text) - 1
                except ValueError:
                    await event.respond("❌ Envoie un **numéro** valide, ou tape `annuler`.")
                    return

                if 0 <= num < len(data.destination_channels):
                    dest = data.destination_channels[num]
                    state_next(OWNER_ID, {"dest": dest})
                    await event.respond(
                        f"⚠️ **Tu vas supprimer TOUS les messages de :**\n\n"
                        f"📬 **{dest['name']}**\n\n"
                        f"Cette action est **irréversible**.\n"
                        f"Tape `oui` pour confirmer, ou `annuler` pour abandonner."
                    )
                else:
                    await event.respond(
                        f"❌ Numéro invalide (1–{len(data.destination_channels)}).\n"
                        f"Réessaie ou tape `annuler`."
                    )

            # ──────────────────────────────────────────────────────────────────
            # CLEARDEST — étape 2 : confirmation
            # ──────────────────────────────────────────────────────────────────
            elif action == "clear_dest" and step == 2:
                if text.lower() not in ("oui", "yes", "o"):
                    state_clear(OWNER_ID)
                    await event.respond("❌ Action annulée. Aucun message supprimé.")
                    return

                dest       = sdata["dest"]
                state_clear(OWNER_ID)
                status_msg = await event.respond(
                    f"🗑️ Suppression en cours dans **{dest['name']}**...\n⏳ Patiente..."
                )
                await _do_cleardest(status_msg, dest)

        except Exception as e:
            logger.error(f"Erreur wizard (action={action}, step={step}): {e}")
            state_clear(OWNER_ID)
            await event.respond(f"❌ Erreur inattendue : {e}")

    # ════════════════════════════════════════════════════════════════════════════
    # Commandes
    # ════════════════════════════════════════════════════════════════════════════

    # ── /annuler ──────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/annuler(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_annuler(event):
        if state_get(OWNER_ID):
            state_clear(OWNER_ID)
            await event.respond("❌ Action annulée.")
        else:
            await event.respond("ℹ️ Aucune action en cours.")

    # ── /start ────────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/start(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_start(event):
        await event.respond(
            "🤖 **Bot de transfert de médias**\n\n"
            "💡 **Démarrage rapide :**\n"
            "1️⃣ `/addcanal` — ajouter une source\n"
            "2️⃣ `/adddestination` — ajouter une destination\n"
            "3️⃣ `/setroute` — configurer le routage\n"
            "4️⃣ `/status` — voir la configuration\n\n"
            "Toutes les commandes fonctionnent **sans argument** — le bot te guide étape par étape !"
        )

    # ── /help ─────────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/help(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_help(event):
        await event.respond(
            "🤖 **Commandes disponibles**\n\n"
            "**📡 Sources**\n"
            "`/addcanal` — ajouter un canal source\n"
            "`/removecanal` — supprimer un canal source\n"
            "`/canaux` — voir les canaux sources\n\n"
            "**📬 Destinations**\n"
            "`/adddestination` — ajouter un canal de réception\n"
            "`/removedestination` — supprimer une destination\n"
            "`/cleardest` — 🗑️ vider tous les messages d'une destination\n"
            "`/destinations` — voir les destinations\n\n"
            "**🔀 Routage**\n"
            "`/setroute` — créer une règle source → destination + filtre\n"
            "`/removeroute` — supprimer une règle\n"
            "`/routes` — voir toutes les règles\n\n"
            "**⚙️ Contrôle**\n"
            "`/pause` — mettre en pause\n"
            "`/resume` — reprendre\n"
            "`/clear` — effacer l'historique des IDs\n"
            "`/annuler` — annuler l'action en cours\n\n"
            "**📥 Historique**\n"
            "`/gethistory` — récupérer tout l'historique d'un canal\n\n"
            "**📊 Infos**\n"
            "`/status` — état complet du bot\n"
            "`/stats` — statistiques de transfert\n\n"
            "💡 Toutes les commandes fonctionnent **sans argument** — je te guide étape par étape !"
        )

    # ── /addcanal ─────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/addcanal(@\w+)?(\s+.+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_addcanal(event):
        arg = (event.pattern_match.group(2) or "").strip()
        if arg:
            # Appel direct avec argument
            try:
                entity       = await resolve_channel(arg)
                channel_id   = entity.id
                channel_name = getattr(entity, "title", arg)
                if any(ch["id"] == channel_id for ch in data.source_channels):
                    await event.respond(f"⚠️ **{channel_name}** est déjà dans la liste.")
                    return
                data.source_channels.append({"id": channel_id, "name": channel_name, "link": arg})
                data.save()
                num = len(data.source_channels)
                await event.respond(
                    f"✅ Source ajoutée (**n°{num}**) : **{channel_name}**\n\n"
                    f"💡 Configure le routage avec `/setroute`"
                )
            except ValueError as e:
                await event.respond(f"❌ {e}")
            except Exception as e:
                await event.respond(f"❌ Erreur : {e}")
        else:
            # Mode wizard
            state_set(OWNER_ID, "add_canal")
            await event.respond(
                "📡 **Ajouter un canal source**\n\n"
                "👇 Envoie le **lien** du canal\n"
                "_(ex: `https://t.me/moncanal` ou `@moncanal`)_\n\n"
                "Tape `annuler` pour abandonner."
            )

    # ── /removecanal ──────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/removecanal(@\w+)?(\s+\d+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_removecanal(event):
        arg = (event.pattern_match.group(2) or "").strip()
        if not data.source_channels:
            await event.respond("📋 Aucun canal source à supprimer.")
            return
        if arg:
            num = int(arg) - 1
            if 0 <= num < len(data.source_channels):
                removed = data.source_channels.pop(num)
                before  = len(data.routes)
                data.routes = [r for r in data.routes if r["source_id"] != removed["id"]]
                removed_routes = before - len(data.routes)
                data.save()
                msg = f"🗑️ Source supprimée : **{removed['name']}**"
                if removed_routes:
                    msg += f"\n⚠️ {removed_routes} règle(s) associée(s) supprimée(s)."
                await event.respond(msg)
            else:
                await event.respond("❌ Numéro invalide.")
        else:
            state_set(OWNER_ID, "remove_canal")
            await event.respond(
                f"🗑️ **Supprimer un canal source**\n\n"
                f"📡 **Sources actuelles :**\n{_fmt_sources()}\n\n"
                f"👇 Envoie le **numéro** à supprimer\n\n"
                f"Tape `annuler` pour abandonner."
            )

    # ── /canaux ───────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/canaux(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_canaux(event):
        if not data.source_channels:
            await event.respond("📋 Aucun canal source.\nUtilise `/addcanal` pour en ajouter.")
            return
        lines = ["📋 **Canaux sources :**\n"]
        for i, ch in enumerate(data.source_channels, 1):
            lines.append(f"{i}. **{ch['name']}** (`{ch.get('link','?')}`)")
        await event.respond("\n".join(lines))

    # ── /adddestination ───────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/adddestination(@\w+)?(\s+.+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_adddestination(event):
        arg = (event.pattern_match.group(2) or "").strip()
        if arg:
            try:
                entity       = await resolve_channel(arg)
                channel_id   = entity.id
                channel_name = getattr(entity, "title", arg)
                if any(d["id"] == channel_id for d in data.destination_channels):
                    await event.respond(f"⚠️ **{channel_name}** est déjà dans les destinations.")
                    return
                data.destination_channels.append({"id": channel_id, "name": channel_name, "link": arg})
                _entity_cache[channel_id] = entity
                data.save()
                num = len(data.destination_channels)
                await event.respond(
                    f"✅ Destination ajoutée (**n°{num}**) : **{channel_name}**\n\n"
                    f"💡 Crée une règle avec `/setroute`"
                )
            except ValueError as e:
                await event.respond(f"❌ {e}")
            except Exception as e:
                await event.respond(f"❌ Erreur : {e}")
        else:
            state_set(OWNER_ID, "add_destination")
            await event.respond(
                "📬 **Ajouter un canal de réception**\n\n"
                "👇 Envoie le **lien** du canal\n"
                "_(ex: `https://t.me/moncanal` ou `@moncanal`)_\n\n"
                "Tape `annuler` pour abandonner."
            )

    # ── /removedestination ────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/removedestination(@\w+)?(\s+\d+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_removedestination(event):
        arg = (event.pattern_match.group(2) or "").strip()
        if not data.destination_channels:
            await event.respond("📋 Aucune destination à supprimer.")
            return
        if arg:
            num = int(arg) - 1
            if 0 <= num < len(data.destination_channels):
                removed = data.destination_channels.pop(num)
                invalidate_entity_cache(removed["id"])
                before = len(data.routes)
                data.routes = [r for r in data.routes if r["dest_id"] != removed["id"]]
                removed_routes = before - len(data.routes)
                data.save()
                msg = f"🗑️ Destination supprimée : **{removed['name']}**"
                if removed_routes:
                    msg += f"\n⚠️ {removed_routes} règle(s) associée(s) supprimée(s)."
                await event.respond(msg)
            else:
                await event.respond("❌ Numéro invalide.")
        else:
            state_set(OWNER_ID, "remove_destination")
            await event.respond(
                f"🗑️ **Supprimer une destination**\n\n"
                f"📬 **Destinations actuelles :**\n{_fmt_destinations()}\n\n"
                f"👇 Envoie le **numéro** à supprimer\n\n"
                f"Tape `annuler` pour abandonner."
            )

    # ── /destinations ─────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/destinations(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_destinations(event):
        if not data.destination_channels:
            await event.respond("📋 Aucune destination.\nUtilise `/adddestination` pour en ajouter.")
            return
        lines = ["📬 **Canaux de réception :**\n"]
        for i, d_ in enumerate(data.destination_channels, 1):
            lines.append(f"{i}. **{d_['name']}** (`{d_.get('link','?')}`)")
        await event.respond("\n".join(lines))

    # ── /cleardest ────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/cleardest(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_cleardest(event):
        if not data.destination_channels:
            await event.respond("📋 Aucune destination configurée.")
            return
        state_set(OWNER_ID, "clear_dest")
        await event.respond(
            f"🗑️ **Vider un canal de réception**\n\n"
            f"📬 **Destinations disponibles :**\n{_fmt_destinations()}\n\n"
            f"👇 Envoie le **numéro** du canal à vider\n\n"
            f"⚠️ Tous les messages seront supprimés définitivement.\n"
            f"Tape `annuler` pour abandonner."
        )

    # ── /setroute ─────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/setroute(@\w+)?(\s+\d+\s+\d+(?:\s+\w+)?)?$", incoming=True, from_users=OWN
    ))
    async def cmd_setroute(event):
        arg = (event.pattern_match.group(2) or "").strip()
        if arg:
            # Appel direct : /setroute 1 2 video
            parts = arg.split()
            if len(parts) < 2:
                await event.respond("❌ Usage : `/setroute <n° source> <n° dest> [photo|video|tout]`")
                return
            try:
                src_num  = int(parts[0]) - 1
                dest_num = int(parts[1]) - 1
                filtre   = parts[2].lower() if len(parts) > 2 else "all"
                if filtre in ("tout", "all"):
                    filtre = "all"
            except ValueError:
                await event.respond("❌ Numéros invalides.")
                return

            if src_num < 0 or src_num >= len(data.source_channels):
                await event.respond(f"❌ Source n°{src_num+1} inexistante.")
                return
            if dest_num < 0 or dest_num >= len(data.destination_channels):
                await event.respond(f"❌ Destination n°{dest_num+1} inexistante.")
                return

            src  = data.source_channels[src_num]
            dest = data.destination_channels[dest_num]
            existing = next(
                (r for r in data.routes if r["source_id"] == src["id"] and r["dest_id"] == dest["id"]),
                None,
            )
            if existing:
                existing["filter"] = filtre
                action_word = "mise à jour"
            else:
                data.routes.append({"source_id": src["id"], "dest_id": dest["id"], "filter": filtre})
                action_word = "créée"
            data.save()
            fl = {"all": "📷🎬 Tout", "photo": "📷 Photos uniquement", "video": "🎬 Vidéos uniquement"}
            await event.respond(
                f"✅ Règle {action_word} !\n\n"
                f"📡 Source : **{src['name']}**\n"
                f"➡️ Destination : **{dest['name']}**\n"
                f"🔍 Filtre : {fl.get(filtre, filtre)}"
            )
        else:
            # Mode wizard
            if not data.source_channels:
                await event.respond("❌ Aucune source. Ajoute-en une avec `/addcanal`.")
                return
            if not data.destination_channels:
                await event.respond("❌ Aucune destination. Ajoute-en une avec `/adddestination`.")
                return
            state_set(OWNER_ID, "set_route", step=1)
            await event.respond(
                f"🔀 **Créer une règle de routage**\n\n"
                f"📡 **Sources disponibles :**\n{_fmt_sources()}\n\n"
                f"👇 Envoie le **numéro** de la source\n\n"
                f"Tape `annuler` pour abandonner."
            )

    # ── /removeroute ──────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/removeroute(@\w+)?(\s+\d+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_removeroute(event):
        arg = (event.pattern_match.group(2) or "").strip()
        if not data.routes:
            await event.respond("📋 Aucune règle à supprimer.")
            return
        if arg:
            num = int(arg) - 1
            if 0 <= num < len(data.routes):
                route     = data.routes.pop(num)
                src_info  = next((c["name"] for c in data.source_channels if c["id"] == route["source_id"]), "?")
                dest_info = next((d["name"] for d in data.destination_channels if d["id"] == route["dest_id"]), "?")
                data.save()
                await event.respond(f"🗑️ Règle supprimée : **{src_info}** → **{dest_info}**")
            else:
                await event.respond("❌ Numéro invalide.")
        else:
            state_set(OWNER_ID, "remove_route")
            await event.respond(
                f"🗑️ **Supprimer une règle**\n\n"
                f"🔀 **Règles actuelles :**\n{_fmt_routes()}\n\n"
                f"👇 Envoie le **numéro** à supprimer\n\n"
                f"Tape `annuler` pour abandonner."
            )

    # ── /routes ───────────────────────────────────────────────────────────────
    @bot_client.on(events.NewMessage(pattern=r"^/routes(@\w+)?$", incoming=True, from_users=OWN))
    async def cmd_routes(event):
        if not data.routes:
            msg = "📋 Aucune règle.\n\nUtilise `/setroute` pour en créer une."
            if data.destination_channels:
                msg += f"\n\n💡 Sans règle → **{data.destination_channels[0]['name']}** (tout)"
            await event.respond(msg)
            return
        fl = {"all": "📷🎬 Tout", "photo": "📷 Photos", "video": "🎬 Vidéos"}
        lines = ["🔀 **Règles de routage :**\n"]
        for i, route in enumerate(data.routes, 1):
            s = next((c["name"] for c in data.source_channels if c["id"] == route["source_id"]), f"id:{route['source_id']}")
            d = next((x["name"] for x in data.destination_channels if x["id"] == route["dest_id"]), f"id:{route['dest_id']}")
            lines.append(f"{i}. **{s}** → **{d}** ({fl.get(route['filter'], route['filter'])})")
        if data.destination_channels:
            lines.append(f"\n💡 Sources sans règle → **{data.destination_channels[0]['name']}** (tout)")
        await event.respond("\n".join(lines))

    # ── /setdestination (rétro-compat) ────────────────────────────────────────
    @bot_client.on(events.NewMessage(
        pattern=r"^/setdestination(@\w+)?\s+(.+)$", incoming=True, from_users=OWN
    ))
    async def cmd_setdestination(event):
        link = event.pattern_match.group(2).strip()
        try:
            entity       = await resolve_channel(link)
            channel_name = getattr(entity, "title", link)
            channel_id   = entity.id
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
        state_str = "⏸️ En pause" if data.paused else "▶️ Actif"
        await event.respond(
            f"📊 **État du bot**\n\n"
            f"État : {state_str}\n\n"
            f"📡 **Sources ({len(data.source_channels)}) :**\n{_fmt_sources()}\n\n"
            f"📬 **Destinations ({len(data.destination_channels)}) :**\n{_fmt_destinations()}\n\n"
            f"🔀 **Règles ({len(data.routes)}) :**\n{_fmt_routes()}\n\n"
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
        pattern=r"^/gethistory(@\w+)?(\s+.+)?$", incoming=True, from_users=OWN
    ))
    async def cmd_gethistory(event):
        arg = (event.pattern_match.group(2) or "").strip()
        if not data.destination_channels:
            await event.respond("❌ Aucune destination configurée.\nUtilise `/adddestination` d'abord.")
            return
        if arg:
            await _do_gethistory(event, arg)
        else:
            # Pré-remplir avec le premier canal source si dispo
            if data.source_channels and data.source_channels[0].get("link"):
                hint = f"\n_(ex: `{data.source_channels[0]['link']}`)_"
            else:
                hint = "\n_(ex: `@moncanal` ou `https://t.me/moncanal`)_"
            state_set(OWNER_ID, "get_history")
            await event.respond(
                f"📥 **Récupérer l'historique**\n\n"
                f"👇 Envoie le **lien** du canal source{hint}\n\n"
                f"Tape `annuler` pour abandonner."
            )


# ── Logique /cleardest ────────────────────────────────────────────────────────

async def _do_cleardest(status_msg, dest: dict):
    """Supprime tous les messages du canal de destination donné."""
    dest_id = dest["id"]
    try:
        entity = await get_entity_cached(dest_id)
        if not entity:
            await status_msg.edit("❌ Canal introuvable ou accès refusé.")
            return

        deleted_total = 0
        batch_size    = 100

        while True:
            msg_ids = []
            async for msg in user_client.iter_messages(entity, limit=batch_size):
                msg_ids.append(msg.id)

            if not msg_ids:
                break

            try:
                await user_client(DeleteMessagesRequest(channel=entity, id=msg_ids))
            except Exception:
                # Fallback : suppression message par message
                for mid in msg_ids:
                    try:
                        await user_client.delete_messages(entity, [mid])
                    except Exception:
                        pass

            deleted_total += len(msg_ids)
            await status_msg.edit(
                f"🗑️ Suppression en cours dans **{dest['name']}**...\n"
                f"✅ {deleted_total} messages supprimés..."
            )

            if len(msg_ids) < batch_size:
                break
            await asyncio.sleep(0.5)

        await status_msg.edit(
            f"✅ **Canal vidé !**\n\n"
            f"📬 Canal : **{dest['name']}**\n"
            f"🗑️ Messages supprimés : **{deleted_total}**\n"
            f"⏰ {now_paris().strftime('%d/%m/%Y à %H:%M')}"
        )
        logger.info(f"Canal vidé: {dest['name']} ({deleted_total} messages supprimés)")

    except Exception as e:
        logger.error(f"Erreur cleardest: {e}")
        await status_msg.edit(f"❌ Erreur lors de la suppression : {e}")


# ── Logique /gethistory ───────────────────────────────────────────────────────

async def _do_gethistory(event, target_link: str):
    """Récupère et envoie l'historique d'un canal source."""
    status_msg = await event.respond("🔍 Connexion au canal en cours...")
    try:
        entity = await resolve_channel(target_link)
    except ValueError as e:
        await status_msg.edit(f"❌ {e}")
        return

    source_name = getattr(entity, "title", target_link)
    source_id   = entity.id

    routes_for_source = data.get_routes_for_source(source_id)
    if routes_for_source:
        dest_names = ", ".join(
            next((d["name"] for d in data.destination_channels if d["id"] == r["dest_id"]), "?")
            for r in routes_for_source
        )
        route_info = f"🔀 Règles : {len(routes_for_source)} destination(s) ({dest_names})"
    else:
        default    = data.get_default_dest()
        dest_names = default["name"] if default else "?"
        route_info = f"📬 Destination par défaut : {dest_names}"

    all_messages     = []
    offset_id        = 0
    total_fetched    = 0
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
            f"Canal : **{source_name}**\nMessages parcourus : {total_fetched}"
        )
        return

    new_messages = [m for m in all_messages if m.id not in data.history_ids]
    skipped      = len(all_messages) - len(new_messages)
    total_new    = len(new_messages)

    if total_new == 0:
        await status_msg.edit(
            f"✅ **Déjà tout envoyé !**\n\n"
            f"Canal : **{source_name}**\n"
            f"Médias trouvés : {len(all_messages)}\nDéjà envoyés : {skipped}"
        )
        return

    await status_msg.edit(
        f"📤 **Envoi en cours**\n"
        f"📺 Canal : **{source_name}**\n"
        f"{route_info}\n\n"
        f"`{'░' * 16}` 0/{total_new}\n"
        f"✅ Envoyés : 0  ❌ Échecs : 0\n"
        f"⚡ Vitesse : calcul...\n⏱ Temps restant : calcul..."
    )

    async def live_progress(s, f, total, speed, eta):
        bar       = _progress_bar(s + f, total)
        speed_str = f"{speed:.1f} médias/min" if speed > 0 else "calcul..."
        await status_msg.edit(
            f"📤 **Envoi en cours**\n"
            f"📺 Canal : **{source_name}**\n"
            f"{route_info}\n\n"
            f"`{bar}` {s + f}/{total}\n"
            f"✅ Envoyés : **{s}**  ❌ Échecs : **{f}**\n"
            f"⚡ Vitesse : {speed_str}\n⏱ Temps restant : ~{eta}"
        )

    sent, failed = await process_message_queue(
        new_messages, source_id, source_name, progress_callback=live_progress,
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


# ── Menu commandes Telegram ───────────────────────────────────────────────────

async def register_bot_commands():
    commands = [
        BotCommand(command="status",            description="État complet du bot"),
        BotCommand(command="canaux",            description="Canaux sources"),
        BotCommand(command="addcanal",          description="Ajouter un canal source"),
        BotCommand(command="removecanal",       description="Supprimer un canal source"),
        BotCommand(command="destinations",      description="Canaux de réception"),
        BotCommand(command="adddestination",    description="Ajouter un canal de réception"),
        BotCommand(command="removedestination", description="Supprimer une destination"),
        BotCommand(command="cleardest",         description="Vider tous les messages d'une destination"),
        BotCommand(command="routes",            description="Règles de routage"),
        BotCommand(command="setroute",          description="Créer une règle source → dest + filtre"),
        BotCommand(command="removeroute",       description="Supprimer une règle"),
        BotCommand(command="gethistory",        description="Récupérer l'historique d'un canal"),
        BotCommand(command="pause",             description="Mettre en pause"),
        BotCommand(command="resume",            description="Reprendre"),
        BotCommand(command="stats",             description="Statistiques"),
        BotCommand(command="clear",             description="Effacer l'historique des IDs"),
        BotCommand(command="annuler",           description="Annuler l'action en cours"),
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
