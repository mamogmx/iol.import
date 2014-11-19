import simplejson as json
import sqlalchemy as sql
import sqlalchemy.orm as orm
import DateTime
import urllib
import os
import time
import datetime

from Products.CMFPlomino.PlominoUtils import DateToString,Now,StringToDate
from Products.CMFCore.utils import getToolByName
from AccessControl.SecurityManagement import newSecurityManager
from mimetypes import MimeTypes
from plone import api

conn_string = "postgres://postgres:postgres@192.168.1.133:5433/sitar"

engine = sql.create_engine(conn_string)
connection = engine.connect()

keysCantieri=['id','numero','username','istruttore','data_presentazione','protocollo','data_protocollo','oggetto','prescrizioni','note','stato_istanze','datainizio','datafine','orario','rinnovoinsanatoria','onerosa','ricevuta','cauzione','ricevutacauzione','svincolo','importo','cognome','nome','indirizzo','comune','prov','cap','telefono','email','comunato','provnato','datanato','sesso','codfis','ragsoc','titolod','sede','comuned','provd','capd','piva','convenzione','data_pagamento','datainizio_eff','datafine_eff','note_utente','motivo_sospensione','n_autorizzazione','data_autorizzazione']
valuesCantieri=['id','numero_pratica','owner','istruttore','data_pratica','numero_protocollo','data_protocollo','cantiere_motivazione','prescrizioni_istruttore','cantieri_nore','wf_iol','autorizzata_dal','autorizzata_al','cantieri_orario','rinnovo_sanatoria','onerosa','ricevuta','cauzione','ricevutacauzione','svincolo','importo','fisica_cognome','fisica_nome','fisica_indirizzo','fisica_comune','fisica_provincia','fisica_cap','fisica_telefono','fisica_email','fisica_comune_nato','fisica_provincia_nato','fisica_data_nato','fisica_sesso','fisica_cf','giuridica_denominazione','titolod','giuridica_indirizzo','giuridica_comune','giuridica_provincia','giuridica_cap','giuridica_piva','convenzione','data_pagamento','datainizio_eff','datafine_eff','note_utente','istruttoria_motivo_sospensione','numero_autorizzazione','data_autorizzazione']

cantieriDict=dict()
for i in range(len(valuesCantieri)):
   cantieriDict[keysCantieri[i]]=valuesCantieri[i]

mappingWF=dict(
    assegnazione_ri = ['protocolla','invia_domanda','assegna'],
    sospensione = ['sospendi'],
    integrazione = ['integra'],
    istruttoria_no = ['preavviso_rigetto'],
    diniego = ['rigetta'],
    acc_osservazioni = ['torna_istruttoria'],
    istruttoria_ok_convenz = ['istruttoria_completata','autorizza'],
    backtoistruttoria = ['torna_istruttoria'],
    istrutoria_ok = ['richiedi_pagamento'],
    pagamenti = ['effettua_pagamento'],
    backtoattesa_pagamenti = ['torna_istruttoria']
)   
   
   
def default(o):
    if type(o) is datetime.date or type(o) is datetime.datetime:
        return o.isoformat()   
   
class plominoData(object):
    def __init__(self, id, plominodb, form, owner, url, review_state, review_history, data):
        self.id = id
        self.plominoform = form
        self.plominodb = plominodb
        self.owner = owner
        self.review_state = review_state
        self.review_history = review_history
        self.url = url
        self.data = data   

        

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, DateTime.DateTime):
            return obj.ISO()
        elif isinstance(obj, datetime.date):
            return obj.isoformat()
        elif isinstance(obj, datetime.timedelta):
            return (datetime.datetime.min + obj).time().isoformat()
        else:
            return super(DateTimeEncoder, self).default(obj)
  
        
        
def getPlominoInfo():
    f = open('cantieri.json','r')
    d = f.read()
    f.close()
    data = json.loads(d.replace('\"','"'))
    return data

def saveData(d,db):
    
    metadata = sql.schema.MetaData(bind=db,reflect=True,schema='istanze')
    table = sql.Table('occupazione_suolo', metadata, autoload=True)
    orm.clear_mappers() 
    rowmapper = orm.mapper(plominoData,table)
    Sess = orm.sessionmaker(bind = db)
    session = Sess()
    data = json.loads(json.dumps(d, default=default,use_decimal=True ))
    row = plominoData(data['id'],data['plominodb'],data['plominoform'],data['owner'],data["url"], data["review_state"], data["review_history"],data['data'])
    session.add(row)
    session.commit()
    session.close()


def populateDB(app):
    psite = app.unrestrictedTraverse("istanze")
    plominodb = psite.iol_cantieri
    query = "SELECT * FROM istanze.occupazione_suolo"
    res = connection.execute(query)
    cantieri = res.fetchall()
    i = 0
    for r in cantieri:
        i+=1
        info = dict(r)
        data = json.loads(info['data'])
        doc = plominodb.createDocument(data['id'])
        doc.setItem('Form','frm_cantieri_base')
        # Setting Items on Document
        for key,val in data.iteritems():
            if key == 'elementi_scavo_dg':
                doc.setItem(key,val.values())
            else:
                doc.setItem(key,val)
        # Setting Ownership
        if data['owner']:
            user = api.user.get(username=data['owner'])
            doc.changeOwnership(user, recursive=False)
        # Setting Roles
            #doc.manage_setLocalRoles([], ["iol-reviewer",])
            #doc.manage_setLocalRoles([], ["iol-manager",])
        #Apply Workflow
        wfInfo = json.loads(data['review_history'])
        for wf in wfInfo:
            if wf['action'] in mappingWF:
                for tr in mappingWF[wf['action']]:
                    pass
        
        
def findData(app):
    psite = app.unrestrictedTraverse("istanze")

    query = "SELECT * FROM istanze_cantieri.istanze order by numero"
    res = connection.execute(query)
    cantieri = res.fetchall()
    result = list()
    i = 0
    infoDB = getPlominoInfo()
    for r in cantieri:
        i+=1
        data = dict()
        data['elementi_scavo_dg'] = list()
        query = "SELECT 0 as tipologia_occupazione,'empty' as elemento_descrizione, lunghezza as occupazione_lunghezza , larghezza as occupazione_larghezza, zona as elemento_zona FROM istanze_cantieri.elementi WHERE istanza='%s'" %r['id']
        res = connection.execute(query)
        elementi = res.fetchall()
        for el in elementi:
            data['elementi_scavo_dg'].append(dict(el))
        for key,val in dict(r).iteritems():
            if key in cantieriDict.keys():
                data[cantieriDict[key]] = val
        
        #d = json.loads(json.dumps(data, default=DateTime.DateTime.ISO,use_decimal=True ))
        id = data['id']
        
        
        if id in infoDB.keys():
            info = infoDB[id]
            dataCantieri = dict(
                id = id,
                plominoform = info['data']['Form'],
                plominodb = 'iol_cantieri',
                owner = data['owner'],
                url = None,
                review_state = info['review_state'],
                review_history = info['review_history'],
                data = data
            )
            try:
                saveData(dataCantieri,connection)
                #print "Considerati %d cantieri" %i  
            except Exception as e:
                print 'Errore record %d : %s' %(i,str(e))
        if (i%100)==0:
            print "Considerati %d cantieri" %i  
           
    return 1
if "app" in locals():
    # Use Zope application server user database (not plone site)
    admin=app.acl_users.getUserById("admin")
    newSecurityManager(None, admin)
    res = findData(app)

    import transaction;
    transaction.commit()
    # Perform ZEO client synchronization (if running in clustered mode)
    app._p_jar.sync()