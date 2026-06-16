# 🤖 Bot Telegram — Transfert de médias

Bot qui surveille des canaux Telegram (même avec téléchargement restreint) et transfère automatiquement les photos et vidéos vers un canal de destination.

## ✨ Fonctionnalités

- ✅ Accès aux canaux avec téléchargement/forwarding restreint
- ✅ Photos envoyées **en photo**, vidéos envoyées **en vidéo** (pas en fichier)
- ✅ Gestion de plusieurs canaux sources
- ✅ Traitement en parallèle (jusqu'à 30+ médias sans bug)
- ✅ Commande `/gethistory` pour récupérer tout l'historique
- ✅ Pause/reprise, statistiques, gestion complète

## 📋 Commandes

| Commande | Description |
|----------|-------------|
| `/addcanal <lien>` | Ajouter un canal source |
| `/removecanal <numéro>` | Supprimer un canal source |
| `/canaux` | Voir les canaux surveillés |
| `/setdestination <lien>` | Définir le canal de destination |
| `/pause` | Mettre en pause les transferts |
| `/resume` | Reprendre les transferts |
| `/clear` | Effacer l'historique des IDs |
| `/gethistory <lien>` | Récupérer tout l'historique médias |
| `/status` | État actuel du bot |
| `/stats` | Statistiques du jour |
| `/help` | Aide |

## 🚀 Installation et déploiement

### Étape 1 — Créer les identifiants Telegram

1. Va sur [my.telegram.org/apps](https://my.telegram.org/apps)
2. Crée une application et note ton `API_ID` et `API_HASH`

### Étape 2 — Créer le bot

1. Parle à [@BotFather](https://t.me/BotFather) sur Telegram
2. Crée un bot avec `/newbot`
3. Note le `BOT_TOKEN`

### Étape 3 — Obtenir ton Telegram ID

1. Parle à [@userinfobot](https://t.me/userinfobot)
2. Il te donnera ton `OWNER_ID`

### Étape 4 — Générer le SESSION_STRING

Le SESSION_STRING permet au bot d'agir en tant qu'utilisateur (nécessaire pour les canaux restreints).

```bash
pip install telethon
python generate_session.py
```

Suis les instructions, connecte-toi avec ton compte Telegram, et copie la chaîne générée.

### Étape 5 — Déployer sur Railway

1. Va sur [railway.app](https://railway.app) et crée un projet
2. Connecte ce dossier (ou fork le repo)
3. Dans **Variables**, ajoute :

```
API_ID=ton_api_id
API_HASH=ton_api_hash
BOT_TOKEN=ton_bot_token
OWNER_ID=ton_id_telegram
SESSION_STRING=ta_session_string
```

4. Railway détectera `railway.toml` et démarrera automatiquement le bot

### Étape 6 — Configuration initiale

Parle à ton bot sur Telegram :

```
/setdestination @ton_canal_destination
/addcanal @canal_source
/status
```

## 🔧 Test en local

```bash
pip install -r requirements.txt
cp .env.example .env
# Remplis les valeurs dans .env
python bot.py
```

## ⚠️ Notes importantes

- **Ne partage jamais ton `SESSION_STRING`** — il donne accès complet à ton compte Telegram
- Le bot doit être **admin** du canal de destination pour pouvoir y poster
- Pour `/gethistory`, les vidéos supprimées ne peuvent être récupérées **que si elles ont été vues/cachées par Telegram** avant la suppression (limitation de l'API)
- Le bot ajoute ton compte comme membre du canal source automatiquement si possible
