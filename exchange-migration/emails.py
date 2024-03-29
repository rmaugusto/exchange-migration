from abc import ABC
from exchangelib import CalendarItem, Message
from exchangelib import EWSDateTime, EWSTimeZone
from exchangelib.items import (
    Message,
    Contact, DistributionList, Persona, MeetingRequest, MeetingResponse, MeetingCancellation
)
from exchangelib import Q

#Abstract Email Migrator
class BaseItem(ABC):
    
    def __init__(self, folder_migrator, logger, tp, config):
        self.folder_migrator = folder_migrator
        self.logger = logger
        self.tp = tp
        self.config = config
        self.task_limit = config['general']['task_limit']


    def query_items_not_copied(self):
        tz = EWSTimeZone('America/Sao_Paulo')
        initial_date = EWSDateTime.from_datetime(self.config['general']['initial_date']).astimezone(tz)
        final_date = EWSDateTime.from_datetime(self.config['general']['final_date']).astimezone(tz)

        return Q(source_id__exists=False) & Q(datetime_received__gte=initial_date) & Q(datetime_received__lte=final_date)
    
    def query_items(self):
        tz = EWSTimeZone('America/Sao_Paulo')
        initial_date = EWSDateTime.from_datetime(self.config['general']['initial_date']).astimezone(tz)
        final_date = EWSDateTime.from_datetime(self.config['general']['final_date']).astimezone(tz)
        return Q(datetime_received__gte=initial_date) & Q(datetime_received__lte=final_date)
     
    def get_item_by_id(self, folder, item_id):
        q = Q(id__exact=item_id)
        items = folder.filter(q).only('id', 'changekey', 'item_class', 'source_id')
        return items.count()

    def exists_item_by_source_id(self, folder, item_id):
        q = Q(source_id__exact=item_id)
        items = folder.filter(q).only('id', 'changekey', 'item_class', 'source_id')
        return items.count() > 0


    def get_item_by_source_id(self, folder, item_id):
        q = Q(source_id__exact=item_id)
        items = folder.filter(q).only('id', 'changekey', 'item_class', 'source_id')

        if items.exists():
            return items.get()
        
        return None

    def is_copy_elegible(self, item):
        copy = False 

        if isinstance(item, Message):
            copy = True
        elif isinstance(item, Contact) or  isinstance(item, DistributionList) or  isinstance(item, Persona) :
            copy = True
        elif isinstance(item, CalendarItem):
            copy = True
        elif isinstance(item, MeetingRequest):
            copy = True
        elif isinstance(item, MeetingResponse):
            copy = True
        elif isinstance(item, MeetingCancellation):
            copy = True

        return copy


class ItemCopier(BaseItem):
    def __init__(self, folder_migrator, logger, tp, config):
        super().__init__(folder_migrator, logger, tp, config)

    def copy_items(self, acc_orig, acc_dest):

        for id, folder  in self.folder_migrator.map_folders.items():

            if folder.origin.absolute == '/root':
                continue

            while True:
                try:
                    self.copied_items = 0

                    q = super().query_items_not_copied()
                    er = folder.origin.filter(q).only('id', 'changekey', 'item_class', 'source_id')
                    er.page_size = 200
                    er.chunk_size = 5
                    total_items = er.count()
                    self.logger.info( f"Processando diretório {folder.origin.absolute} com {total_items} itens" )
                    submitted_total = 0
                    items_covered = 0
                    for item in er:
                        
                        submitted_total += 1
                        items_covered += 1

                        #print(item)
                        #self.tp.add_task(self.process_item, acc_orig, acc_dest, item)
                        self.copy_item(acc_orig, acc_dest, item)

                        if submitted_total == self.task_limit:
                            self.tp.wait_completion()
                            self.logger.info( f"Processando diretório {folder.origin.absolute} - {items_covered}/{total_items} " )
                            submitted_total = 0

                    self.tp.wait_completion()

                    if self.copied_items == 0:
                        print( f"Diretório sem mais items {folder.origin.absolute}" )
                        break
                except Exception as e:
                    self.logger.warn("Erro, continuando...", e) 


    def copy_item(self, acc_orig, acc_dest, item, forceCopy=False):
        
        if super().is_copy_elegible(item):

            if isinstance(item, Message) or isinstance(item, MeetingRequest) or isinstance(item, MeetingResponse) or isinstance(item, MeetingCancellation):
                items = acc_orig.fetch(ids=[(item.id, item.changekey)], only_fields=['parent_folder_id', 'subject', 'id', 'changekey', 'item_class', 'source_id'])
                item = next(items, None)
                parent = item.parent_folder_id.id
                dest_folder = self.folder_migrator.map_folders[parent].dest
            elif isinstance(item, Contact) or  isinstance(item, DistributionList) or  isinstance(item, Persona) :
                dest_folder = acc_dest.contacts
            elif isinstance(item, CalendarItem):
                dest_folder = acc_dest.calendar

            if forceCopy or not super().exists_item_by_source_id(dest_folder, item.id):
                try:
                    item.source_id = item.id
                    item.save(update_fields=["source_id"])

                    subject = ''
                    if hasattr(item, 'subject'):
                        subject = item.subject
                    elif hasattr(item, 'display_name'):
                        subject = item.display_name
                    else:
                        pass

                    print( f"Copiando item {item.id}: - {dest_folder.absolute}/{subject}" )
                    
                    if hasattr(self, 'copied_items'):
                        self.copied_items += 1

                    data = acc_orig.export([item])
                    acc_dest.upload((dest_folder, d) for d in data)
                except Exception as e:
                    self.logger.warn("Erro ao copiar, continuando...", e)
            else:
                print( f"Ignorando item pois já foi copiado {item.id}" )
        else:
            pass


class ItemComparator(BaseItem):
    def __init__(self, folder_migrator, logger, tp, config):
        super().__init__(folder_migrator, logger, tp, config)

    def compare_items(self, acc_orig, acc_dest):

        with open('logs/items_stats.csv', 'a') as f:

            for id, folder  in self.folder_migrator.map_folders.items():

                if folder.origin.absolute == '/root':
                    continue

                try:
                    q = super().query_items()
                    er = folder.origin.filter(q).only('id', 'changekey', 'item_class', 'source_id')
                    er.page_size = 200
                    er.chunk_size = 5
                    self.logger.info( f"Comparando diretório {folder.origin.absolute}" )
                    submitted_total = 0
                    items_covered = 0
                    for item in er:
                        
                        submitted_total += 1
                        items_covered += 1

                        #print(item)
                        self.tp.add_task(self.compare_item, acc_orig, acc_dest, item)
                        #self.compare_item(acc_orig, acc_dest, item)

                        if submitted_total == self.task_limit:
                            self.tp.wait_completion()
                            self.logger.info( f"Processando diretório {folder.origin.absolute} - {items_covered}" )
                            submitted_total = 0

                    self.tp.wait_completion()

                except Exception as e:
                    self.logger.warn("Erro, continuando...", e) 

                    
    def compare_item(self, acc_orig, acc_dest, item):
        
        if super().is_copy_elegible(item):

            if isinstance(item, Message) or isinstance(item, MeetingRequest) or isinstance(item, MeetingResponse) or isinstance(item, MeetingCancellation):
                items = acc_orig.fetch(ids=[(item.id, item.changekey)], only_fields=['parent_folder_id', 'subject', 'id', 'changekey', 'item_class', 'source_id'])
                item = next(items, None)
                parent = item.parent_folder_id.id
                dest_folder = self.folder_migrator.map_folders[parent].dest
            elif isinstance(item, Contact) or  isinstance(item, DistributionList) or  isinstance(item, Persona) :
                dest_folder = acc_dest.contacts
            elif isinstance(item, CalendarItem):
                dest_folder = acc_dest.calendar

            item_dest = super().get_item_by_source_id(dest_folder, item.id)

            if item_dest is None:
                self.logger.warn( f"Item {item.id} não encontrado no destino durante verificação, copiando..." )
                email_copier = ItemCopier(self.folder_migrator, self.logger, self.tp, self.config)
                email_copier.copy_item(acc_orig, acc_dest, item)


        