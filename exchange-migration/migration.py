from utils import AccountManager, FolderDiscovery, getLogger
import time
from emails import ItemCopier
import time


class EmailMigrator:
    def __init__(self):
        pass

    def run(self, config, origin_email, dest_email, initial_date, final_date, action):

        account_manager = AccountManager()
        account_manager.setup(config, origin_email, dest_email)

        print(f"Descobrindo diretórios conhecidos...") 

        folder_migrator = FolderDiscovery(auto_create=True)

        folder_migrator.add_messages_folder(account_manager.acc_orig, account_manager.acc_dest)
        folder_migrator.add_contacts_folder(account_manager.acc_orig, account_manager.acc_dest)
        folder_migrator.add_calendars_folder(account_manager.acc_orig, account_manager.acc_dest)

        #Cria sub dos diretorios conhecidos
        folder_migrator.traverse_and_create(account_manager.acc_orig.inbox, account_manager.acc_dest.inbox)
        folder_migrator.traverse_and_create(account_manager.acc_orig.outbox, account_manager.acc_dest.outbox)
        folder_migrator.traverse_and_create(account_manager.acc_orig.sent, account_manager.acc_dest.sent)

        print(f"Descobrindo diretórios customizados...") 
        folder_migrator.add_first_level(account_manager.acc_orig, account_manager.acc_dest)

        initial_time = time.time()

        if action == 'migration' or action == 'migration_compare':
            print(f"Iniciando copia de emails...")
            copier = ItemCopier()
            copier.copy_items(folder_migrator, config, initial_date, final_date, origin_email, dest_email)

#        if action == 'compare' or action == 'migration_compare':
#            print(f"Gerando estatisticas...")
#            stats = ItemComparator(self.folder_migrator, config, initial_date, final_date)
#            stats.compare_items(acc_orig, acc_dest)

        total_time = time.time() - initial_time
        hours = int(total_time // 3600)
        minutes = int((total_time % 3600) // 60)
        seconds = int(total_time % 60)
        
        # Formatando o tempo no formato hh:mm:ss
        print(f"Finalizado em {hours:10d}:{minutes:02d}:{seconds:02d}") 
    