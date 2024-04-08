import time
from exchangelib import CalendarItem, Folder, Message
from exchangelib.items import (
    Message,
    Contact,  MeetingRequest, MeetingCancellation, MeetingResponse
)
from utils import CustomFieldSourceId, getLogger
from database import Database
from yaml import load, Loader
from migration import EmailMigrator

def main():

    config = load(open('config.yaml', 'r'), Loader=Loader)

    db = Database(config)

    print("Conectando ao banco de dados...")
    db.connect()

    if 'accounts' in config:
        accounts = config['accounts']
        for account_idx in range(0, len(accounts)):
            em = EmailMigrator(db)
            em.run(config, accounts[account_idx]['origin'], accounts[account_idx]['dest'], accounts['general']['date_begin'], accounts['general']['date_end'])
    else:
        while True:
            print("Procurando proxima migração...")
            next_migration = db.get_next_migration(config['general']['instance_name'])

            if next_migration is None:
                print("Nenhuma migração encontrada, aguardando 30 segundos...")
                time.sleep(30)
            else:
                print(f"Tentando travar migração {next_migration.id}...")
                db.try_lock_migration(next_migration.id, config['general']['instance_name'])

                current_migration = db.get_migration_by_id(next_migration.id)

                if current_migration.instance == config['general']['instance_name'] and current_migration.status == 'processing':
                    print(f"Migração {next_migration.id} travada, iniciando migração...")

                    try:
                        em = EmailMigrator()
                        em.run(config, next_migration.email_origin, next_migration.email_destination, next_migration.date_begin, next_migration.date_end, current_migration.action)
                        db.update_migration_done(next_migration.id)
                    except Exception as e:
                        print(f"Erro ao migração {next_migration.id}: {e}")

                else:
                    print(f"Migração {next_migration.id} travada pelo processo {current_migration.instance}, ignorando...")

if __name__ == "__main__":
    main()
