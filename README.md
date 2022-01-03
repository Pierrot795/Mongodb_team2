# Mongodb_team2

Etapes pour lancer l'app en local:

1) activer l'environnement virtuel (accès aux bon packages etc...)
-> sur pc:  dans le dossier principal : source new_env/Scripts/activate (bash)

2) lancer l'app (port 5000 par défaut):
-> flask run

Pour avoir le temps d'execution des queries: aller dans le dossier principal et dans un shell:
-> pathToPython.exe queries_execution_time.py

Si problème avec l'environnement virtuel: installer les packages suivants:
- flask
- pymongo
- python-dotenv