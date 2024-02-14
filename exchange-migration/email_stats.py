from datetime import datetime, timedelta
import time
from exchangelib import EWSDateTime, EWSTimeZone, FaultTolerance, Configuration
from exchangelib import IMPERSONATION, Account,  OAuth2Credentials
from exchangelib import Q
from utils import FolderDiscovery, getLogger

class EmailStats:
    def __init__(self):
        self.logger = None

    def run(self, config, account_idx):

        self.processed_total = 0

        credentials_orig = OAuth2Credentials(
            client_id=config['origin']['client_id'], client_secret=config['origin']['client_secret'], tenant_id=config['origin']['tenant_id']
        )
        config_orig = Configuration(
            retry_policy=FaultTolerance(max_wait=3600), credentials=credentials_orig, max_connections=config['origin']['connection_total']
        )

        origin_email = config['accounts'][account_idx]['origin']
        dest_email = config['accounts'][account_idx]['dest']

        self.logger = getLogger(dest_email)

        self.logger.info("Iniciando leitura dos dados...")
        self.logger.info(f"Origem: {origin_email}")

        try:
            acc_orig = Account(origin_email, credentials=credentials_orig, autodiscover=True,  access_type=IMPERSONATION, config=config_orig)
        except Exception as e:
            self.logger.error(f"Erro ao conectar: {e}")
            return

        self.folder_migrator = FolderDiscovery(logger=self.logger, auto_create=False)

        self.folder_migrator.add_messages_folder(acc_orig, None)
        self.folder_migrator.add_contacts_folder(acc_orig, None)
        self.folder_migrator.add_calendars_folder(acc_orig, None)

        #Cria sub dos diretorios conhecidos
        self.folder_migrator.traverse_and_create(acc_orig.inbox, None)
        self.folder_migrator.traverse_and_create(acc_orig.outbox, None)
        self.folder_migrator.traverse_and_create(acc_orig.sent, None)

        self.folder_migrator.add_first_level(acc_orig, None)

        initial_time = time.time()

        self.stat_items(acc_orig, acc_orig.root, None)

        total_time = time.time() - initial_time
        hours = int(total_time // 3600)
        minutes = int((total_time % 3600) // 60)
        seconds = int(total_time % 60)
        
        # Formatando o tempo no formato hh:mm:ss
        self.logger.info(f"Finalizado em {hours:10d}:{minutes:02d}:{seconds:02d}") 
    

    def stat_items(self, acc_orig, folder_ori, acc_dest):

        with open('logs/emails_total.csv', 'a') as f:

            for id, folder  in self.folder_migrator.map_folders.items():

                if folder.origin.absolute == '/root':
                    continue

                tz = EWSTimeZone('America/Sao_Paulo')
                date_init = EWSDateTime.from_datetime(datetime.now()).astimezone(tz) - timedelta(days=365)
                q = Q(datetime_received__gte=date_init)
                count = folder.origin.filter(q).count()

                self.logger.info( f"{folder.origin.absolute} - {count}" )   

                f.write(f"{acc_orig.primary_smtp_address};{folder.origin.absolute};{count}\n")

