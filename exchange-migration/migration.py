from utils import FolderDiscovery, getLogger
import time
from exchangelib import FaultTolerance, Configuration
from exchangelib import IMPERSONATION, Account, OAuth2Credentials
from thread import ThreadPool
from emails import ItemCopier, ItemComparator
import time


class EmailMigrator:
    def __init__(self):
        pass

    def run(self, config, account_idx):

        self.tp = ThreadPool(config['general']['thread_count'])
        self.copied_items = 0

        credentials_orig = OAuth2Credentials(
            client_id=config['origin']['client_id'], client_secret=config['origin']['client_secret'], tenant_id=config['origin']['tenant_id']
        )
        config_orig = Configuration(
            retry_policy=FaultTolerance(max_wait=3600), credentials=credentials_orig, max_connections=config['origin']['connection_total']
        )

        credentials_dest = OAuth2Credentials(
            client_id=config['dest']['client_id'], client_secret=config['dest']['client_secret'], tenant_id=config['dest']['tenant_id']
        )
        config_dest = Configuration(
            retry_policy=FaultTolerance(max_wait=3600), credentials=credentials_dest, max_connections=config['dest']['connection_total']
        )

        origin_email = config['accounts'][account_idx]['origin']
        dest_email = config['accounts'][account_idx]['dest']

        self.logger = getLogger(dest_email)

        self.logger.info("Iniciando copia de emails...")
        self.logger.info(f"Conectando caixa de origem: {origin_email}")

        try:
            acc_orig = Account(origin_email, credentials=credentials_orig, autodiscover=True,  access_type=IMPERSONATION, config=config_orig)
        except Exception as e:
            self.logger.error(f"Erro ao conectar caixa de origem: {origin_email}, {e}")
            return
        
        self.logger.info(f"Conectando caixa de destino: {dest_email}")
        try:
            acc_dest = Account(dest_email, credentials=credentials_dest, autodiscover=True,  access_type=IMPERSONATION, config=config_dest)
        except Exception as e:
            self.logger.error(f"Erro ao conectar caixa de destino: {dest_email}, {e}")
            return

        self.logger.info(f"Descobrindo diretórios conhecidos...") 

        self.folder_migrator = FolderDiscovery(logger=self.logger, auto_create=True)

        self.folder_migrator.add_messages_folder(acc_orig, acc_dest)
        self.folder_migrator.add_contacts_folder(acc_orig, acc_dest)
        self.folder_migrator.add_calendars_folder(acc_orig, acc_dest)

        #Cria sub dos diretorios conhecidos
        self.folder_migrator.traverse_and_create(acc_orig.inbox, acc_dest.inbox)
        self.folder_migrator.traverse_and_create(acc_orig.outbox, acc_dest.outbox)
        self.folder_migrator.traverse_and_create(acc_orig.sent, acc_dest.sent)

        self.logger.info(f"Descobrindo diretórios customizados...") 
        self.folder_migrator.add_first_level(acc_orig, acc_dest)

        initial_time = time.time()

        self.logger.info(f"Iniciando copia de emails...")
        copier = ItemCopier(self.folder_migrator, self.logger, self.tp, config)
        copier.copy_items(acc_orig, acc_dest)

        self.logger.info(f"Gerando estatisticas...")
        stats = ItemComparator(self.folder_migrator, self.logger, self.tp, config)
        stats.compare_items(acc_orig, acc_dest)

        total_time = time.time() - initial_time
        hours = int(total_time // 3600)
        minutes = int((total_time % 3600) // 60)
        seconds = int(total_time % 60)
        
        # Formatando o tempo no formato hh:mm:ss
        self.logger.info(f"Finalizado em {hours:10d}:{minutes:02d}:{seconds:02d}") 
    
        self.tp.close()
