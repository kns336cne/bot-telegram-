async def send_media_to_destination(message, caption_override: str = None) -> bool:
    """Télécharge le média et le renvoie en gérant les erreurs de manière transparente."""
    dest_entity = await get_dest_entity()
    if not dest_entity:
        logger.warning("Destination non configurée ou introuvable")
        return False

    caption = caption_override if caption_override is not None else (message.text or "")
    os.makedirs("tmp", exist_ok=True)
    
    # Détermination propre de l'extension
    ext = "jpg"
    if isinstance(message.media, MessageMediaDocument):
        mime = message.media.document.mime_type or ""
        if "video" in mime:
            ext = "mp4"
        elif "gif" in mime:
            ext = "gif"
        elif "/" in mime:
            ext = mime.split("/")[-1]
    elif isinstance(message.media, MessageMediaPhoto):
        ext = "jpg"

    tmp_filepath = f"tmp/media_{message.id}.{ext}"

    try:
        # 1. Téléchargement
        async with DOWNLOAD_SEMAPHORE:
            path = await user_client.download_media(message, file=tmp_filepath)

        if not path or not os.path.exists(path):
            logger.warning(f"Le téléchargement a échoué ou le fichier est vide pour msg_id={message.id}")
            return False

        # 2. Envoi
        async with UPLOAD_SEMAPHORE:
            try:
                await user_client.send_file(dest_entity, file=path, caption=caption)
                return True
            except Exception as e_user:
                logger.warning(f"user_client a échoué ({e_user}), tentative via bot_client...")
                try:
                    await bot_client.send_file(dest_entity, file=path, caption=caption)
                    return True
                except Exception as e_bot:
                    logger.error(f"bot_client a également échoué: {e_bot}")
                    return False

    except FloodWaitError as e:
        logger.warning(f"FloodWait détecté: attente de {e.seconds}s")
        await asyncio.sleep(e.seconds + 1)
        return await send_media_to_destination(message, caption_override)
    except Exception as e:
        # TRÈS IMPORTANT : Capture l'erreur exacte pour la mettre dans la console
        logger.error(f"💥 ERREUR CRITIQUE lors du transfert du msg_id={message.id}: {e}", exc_info=True)
        return False
    finally:
        # Nettoyage du fichier pour ne pas saturer le serveur
        if os.path.exists(tmp_filepath):
            try:
                os.remove(tmp_filepath)
            except Exception:
                pass


async def send_album_to_destination(messages: list, source_name: str) -> int:
    """Télécharge et renvoie un album complet avec gestion robuste des erreurs."""
    dest_entity = await get_dest_entity()
    if not dest_entity:
        return 0

    os.makedirs("tmp", exist_ok=True)

    async def dl_one(msg):
        ext = "jpg"
        if isinstance(msg.media, MessageMediaDocument):
            mime = msg.media.document.mime_type or ""
            if "video" in mime:
                ext = "mp4"
            elif "gif" in mime:
                ext = "gif"
        filepath = f"tmp/album_{msg.id}.{ext}"
        try:
            async with DOWNLOAD_SEMAPHORE:
                return await user_client.download_media(msg, file=filepath)
        except Exception as e:
            logger.warning(f"Échec du téléchargement pour l'album (msg_id={msg.id}): {e}")
            return None

    results = await asyncio.gather(*[dl_one(m) for m in messages])
    valid_paths = [p for p in results if p and os.path.exists(p)]

    if not valid_paths:
        return 0

    caption = f"📺 Source: {source_name}\n📅 {now_paris().strftime('%d/%m/%Y à %H:%M')}"
    captions = [caption] + [""] * (len(valid_paths) - 1)

    count = 0
    try:
        async with UPLOAD_SEMAPHORE:
            try:
                await user_client.send_file(dest_entity, file=valid_paths, caption=captions)
                count = len(valid_paths)
            except Exception as e_user:
                logger.warning(f"Envoi de l'album par l'utilisateur échoué ({e_user}), essai via le bot...")
                try:
                    await bot_client.send_file(dest_entity, file=valid_paths, caption=captions)
                    count = len(valid_paths)
                except Exception as e_bot:
                    logger.error(f"Échec total de l'album via bot: {e_bot}")
                    # Mode secours : On tente d'envoyer un par un
                    for msg in messages:
                        if await send_media_to_destination(msg):
                            count += 1
    except Exception as e:
        logger.error(f"Erreur générale dans l'album: {e}", exc_info=True)
    finally:
        for path in valid_paths:
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass

    return count
