from abc import ABC
from functools import partial
import time
from exchangelib import CalendarItem, Message
from exchangelib import EWSDateTime, EWSTimeZone
from exchangelib.items import (
    Message,
    Contact, DistributionList, Persona, MeetingRequest, MeetingResponse, MeetingCancellation
)
from exchangelib import Q
from multiprocessing import Pool
from database import Database
from utils import AccountManager, CustomFieldSourceId
from os import getpid

#Abstract Email Migrator
class QueryUtils():
    
    def query_items_not_copied(initial_date, final_date):
        tz = EWSTimeZone('America/Sao_Paulo')

        initial_date = EWSDateTime.from_datetime(initial_date).astimezone(tz)
        final_date = EWSDateTime.from_datetime(final_date).astimezone(tz)

        #initial_date = EWSDateTime.from_datetime(self.config['general']['initial_date']).astimezone(tz)
        #final_date = EWSDateTime.from_datetime(self.config['general']['final_date']).astimezone(tz)
        q_types = Q(item_class='IPM.Appointment') | Q(item_class='IPM.Contact') | Q(item_class='IPM.DistList') | Q(item_class='IPM.Note') | Q(item_class='IPM.StickyNote') | Q(item_class='IPM.Schedule.Meeting.Canceled') | Q(item_class='IPM.Schedule.Meeting.Request') | Q(item_class='IPM.Schedule.Meeting.Resp.Neg') | Q(item_class='IPM.Schedule.Meeting.Resp.Pos') | Q(item_class='IPM.Schedule.Meeting.Resp.Tent') | Q(item_class='IPM.Task') | Q(item_class='IPM.TaskRequest.Accept') | Q(item_class='IPM.TaskRequest.Decline') | Q(item_class='IPM.TaskRequest') | Q(item_class='IPM.TaskRequest.Update')

        return Q(source_id__exists=False) & Q(datetime_received__gte=initial_date) & Q(datetime_received__lte=final_date) & q_types
    
    def query_items(initial_date, final_date):
        tz = EWSTimeZone('America/Sao_Paulo')

        initial_date = EWSDateTime.from_datetime(initial_date).astimezone(tz)
        final_date = EWSDateTime.from_datetime(final_date).astimezone(tz)

        #initial_date = EWSDateTime.from_datetime(self.config['general']['initial_date']).astimezone(tz)
        #final_date = EWSDateTime.from_datetime(self.config['general']['final_date']).astimezone(tz)
        q_types = Q(item_class='IPM.Appointment') | Q(item_class='IPM.Contact') | Q(item_class='IPM.DistList') | Q(item_class='IPM.Note') | Q(item_class='IPM.StickyNote') | Q(item_class='IPM.Schedule.Meeting.Canceled') | Q(item_class='IPM.Schedule.Meeting.Request') | Q(item_class='IPM.Schedule.Meeting.Resp.Neg') | Q(item_class='IPM.Schedule.Meeting.Resp.Pos') | Q(item_class='IPM.Schedule.Meeting.Resp.Tent') | Q(item_class='IPM.Task') | Q(item_class='IPM.TaskRequest.Accept') | Q(item_class='IPM.TaskRequest.Decline') | Q(item_class='IPM.TaskRequest') | Q(item_class='IPM.TaskRequest.Update')
        return Q(datetime_received__gte=initial_date) & Q(datetime_received__lte=final_date) & q_types
     
    def get_item_by_id(folder, item_id):
        q = Q(id__exact=item_id)
        items = folder.filter(q).only('id', 'changekey', 'item_class', 'source_id')
        return items.count()

    def exists_item_by_source_id(folder, item_id):
        q = Q(source_id__exact=item_id)
        items = folder.filter(q).only('id', 'changekey', 'item_class', 'source_id')
        c = items.count()
        return c


    def get_item_by_source_id(folder, item_id):
        q = Q(source_id__exact=item_id)
        items = folder.filter(q).only('id', 'changekey', 'item_class', 'source_id')

        if items.exists():
            return items.get()
        
        return None

    def is_copy_elegible(item):
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


class ItemCopier():

    def init_process(fm, config, initial_date, final_date, origin_email, dest_email):

        global proc_registered
        global proc_init
        global folder_migrator
        global db
        global account_manager


        if 'proc_registered' not in globals():

            proc_registered = True
            proc_init = True

            try:
                Contact.register('source_id', CustomFieldSourceId)
                CalendarItem.register('source_id', CustomFieldSourceId)
                Message.register('source_id', CustomFieldSourceId)
                MeetingRequest.register('source_id', CustomFieldSourceId)
                MeetingResponse.register('source_id', CustomFieldSourceId)
                MeetingCancellation.register('source_id', CustomFieldSourceId)
            except Exception as e:
                print(f"source_id já registrado: {e}")


            folder_migrator = fm

            db = Database(config)
            db.connect()

            account_manager = AccountManager()
            account_manager.setup(config, origin_email, dest_email)

            proc_init = False


    def copy_item(folder, origin_email, dest_email, item):
        global db
        global account_manager
        global proc_init
        global folder_migrator

        while proc_init:
            time.sleep(1)

        if QueryUtils.is_copy_elegible(item):

            text_type = ''
            if isinstance(item, Message) or isinstance(item, MeetingRequest) or isinstance(item, MeetingResponse) or isinstance(item, MeetingCancellation):
                items = account_manager.acc_orig.fetch(ids=[(item.id, item.changekey)], only_fields=['parent_folder_id', 'subject', 'id', 'changekey', 'item_class', 'source_id'])
                item = next(items, None)
                parent = item.parent_folder_id.id
                dest_folder = folder_migrator.map_folders[parent].dest
                text_type = 'message'
            elif isinstance(item, Contact) or  isinstance(item, DistributionList) or  isinstance(item, Persona) :
                dest_folder = account_manager.acc_dest.contacts
                text_type = 'contact'
            elif isinstance(item, CalendarItem):
                dest_folder = account_manager.acc_dest.calendar
                text_type = 'calendar'

            if QueryUtils.exists_item_by_source_id(dest_folder, item.id) == 0:
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

                    # get child pid process of multiprocessing
                    child_pid = getpid()

                    print( f"PID {child_pid}: Copiando item {item.id}: - {dest_folder.absolute}/{subject}" )
                    
                    db.insert_migration(account_manager.acc_orig.primary_smtp_address, text_type, subject, item.id, None, 'read', dest_folder.absolute)
                    data = account_manager.acc_orig.export([item])

                    db.insert_migration(account_manager.acc_orig.primary_smtp_address, text_type, subject, item.id, None, 'export', dest_folder.absolute)

                    account_manager.acc_dest.upload((dest_folder, d) for d in data)

                    new_id = QueryUtils.get_item_by_source_id(dest_folder, item.id)
                    db.insert_migration(account_manager.acc_orig.primary_smtp_address, text_type, subject, item.id, new_id.id, 'upload', dest_folder.absolute)

                except Exception as e:
                    #print stack trace
                    import traceback
                    traceback.print_exc()
                    #print("Erro ao copiar, continuando...", e)

            else:
                print( f"Ignorando item pois já foi copiado {item.id}" )

    def copy_items(self, folder_migrator, config, initial_date, final_date, origin_email, dest_email):

        thread_count = config['general']['thread_count']
        with Pool(processes=thread_count, initializer=ItemCopier.init_process,initargs=(folder_migrator, config, initial_date, final_date, origin_email, dest_email,) ) as pool:

            for id, folder  in folder_migrator.map_folders.items():

                if folder.origin.absolute == '/root':
                    continue

                q = QueryUtils.query_items_not_copied(initial_date, final_date)
                er = folder.origin.filter(q).only('id', 'changekey', 'item_class', 'source_id')
                er.page_size = 10
                er.chunk_size = 5

                total_items = er.count()
                results = []
                
                if total_items == 0:
                    print( f"Ignorando diretório {folder.origin.absolute} com 0 itens" )
                    continue
                else:
                    print( f"Processando diretório {folder.origin.absolute} com {total_items} itens" )

                    
                    for item in er:
                        result = pool.apply_async(ItemCopier.copy_item, (folder, origin_email, dest_email,item,))
                        results.append(result)

                    [r.wait() for r in results]


