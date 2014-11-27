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
from Products.CMFCore.WorkflowCore import WorkflowException


conn_string_sit = "postgres://Admin:debo@127.0.0.1:5432/sitar"
conn_string_iol = "postgres://postgres:postgres@10.95.10.27:5432/sitar"

doc_base_path = "/zope/portale/parts/cantieri/documenti"

keysCantieri=['id','numero','username','istruttore','data_presentazione','protocollo','data_protocollo','oggetto','prescrizioni','note','stato_istanze','datainizio','datafine','orario','rinnovoinsanatoria','onerosa','ricevuta','cauzione','ricevutacauzione','svincolo','importo','cognome','nome','indirizzo','comune','prov','cap','telefono','email','comunato','provnato','datanato','sesso','codfis','ragsoc','titolod','sede','comuned','provd','capd','piva','convenzione','data_pagamento','datainizio_eff','datafine_eff','note_utente','motivo_sospensione','n_autorizzazione','data_autorizzazione']
valuesCantieri=['id','numero_pratica','owner','istruttore','data_pratica','numero_protocollo','data_protocollo','cantiere_motivazione','prescrizioni_istruttore','cantieri_nore','wf_iol','autorizzata_dal','autorizzata_al','cantieri_orario','rinnovo_sanatoria','onerosa','ricevuta','cauzione','ricevutacauzione','svincolo','importo','fisica_cognome','fisica_nome','fisica_indirizzo','fisica_comune','fisica_provincia','fisica_cap','fisica_telefono','fisica_email','fisica_comune_nato','fisica_provincia_nato','fisica_data_nato','fisica_sesso','fisica_cf','giuridica_denominazione','titolod','giuridica_indirizzo','giuridica_comune','giuridica_provincia','giuridica_cap','giuridica_piva','convenzione','data_pagamento','datainizio_eff','datafine_eff','note_utente','istruttoria_motivo_sospensione','numero_autorizzazione','data_autorizzazione']

cantieriDict=dict()
for i in range(len(valuesCantieri)):
   cantieriDict[keysCantieri[i]]=valuesCantieri[i]

mappingWF=dict(
    assegnazione_ri = ['protocolla','invia_domanda','assegna'],
    start_istruttore = ['protocolla','invia_domanda','assegna'],
    sospensione = ['sospendi'],
    integrazione = ['integra'],
    istruttoria_no = ['preavviso_rigetto'],
    diniego = ['rigetta'],
    acc_osservazioni = ['torna_istruttoria'],
    istruttoria_ok_convenz = ['istruttoria_completata','autorizza'],
    backtoistruttoria = ['torna_istruttoria'],
    istruttoria_ok = ['istruttoria_completata'],
    pagamenti = ['autorizza'],
    #backtoattesa_pagamenti = ['torna_istruttoria']
)   
   
   
def default(o):
    if type(o) is datetime.date or type(o) is datetime.datetime:
        return o.isoformat()   
   
class plominoData(object):
    def __init__(self, id, plominodb, form, owner, url, review_state, review_history,iol_owner,iol_reviewer,iol_manager, data):
        self.id = id
        self.plominoform = form
        self.plominodb = plominodb
        self.owner = owner
        self.review_state = review_state
        self.review_history = review_history
        self.url = url
        self.iol_owner = iol_owner
        self.iol_reviewer = iol_reviewer
        self.iol_manager = iol_manager
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

def getDocuments(doc):
    id = doc.getId()
    path = "%s/%s" %(doc_base_path,id)
    if os.path.exists(path):
        listDocs = [ f for f in os.listdir(path) if os.path.isfile(os.path.join(path,f)) ]
        tot = len(listDocs)
        i = 0
        for name in listDocs:
            
            i += 1
            f = open("%s/%s" %(path,name),'r')
            text = f.read()
            f.close()
            doc.setfile(text,filename=name,contenttype='application/pdf')
            
        print "\tRecuperati %s file su %s" %(i,tot)
    return 1
    
def importDocuments(app):
    psite = app.unrestrictedTraverse("istanze")
    plominodb = psite.iol_cantieri
    j = 0
    for doc in plominodb.getAllDocuments():
        j += 1
        print "%d) Documento %s" %(j,doc.getId())
        getDocuments(doc)
    return 1
    
def geomUpdateDoc(app):
    psite = app.unrestrictedTraverse("istanze")
    #Connessione al Database sitar su SIT
    engine = sql.create_engine(conn_string_sit)
    connection = engine.connect()
    plominodb = psite.iol_cantieri
    i = 0
    for doc in plominodb.getAllDocuments():
        i += 1
        id = doc.getId()
        geom = doc.getItem('geometry','')
        if not geom:
            geom = getGeometry(connection,id) or ''
        print "%d) Aggiornamento geometria %s del documento %s" %(i,geom,id)
        doc.setItem('geometry',geom)
        if  geom.find('POINT')>0:
            tipo = 'punto'
        else:
            tipo = 'linea'
        doc.setItem('elemento_tipo',tipo)
    return 1

def getGeometry(conn,id):
    query = "SELECT id, istanza,'SRID=4326;'||astext(transform(the_geom,4326)) as geometry FROM istanze_cantieri.elementi  WHERE not the_geom is null AND istanza = '%s'" %id
    res = conn.execute(query)
    geometries = res.fetchall()
    if geometries:
        return geometries[0]['geometry']
    else:
        return None
            
def populateDB(app):
    psite = app.unrestrictedTraverse("istanze")
    #Connessione al Database sitar su SIT
    engine = sql.create_engine(conn_string_sit)
    connection = engine.connect()
    #Connessione al Database sitar su IOL
    engine_iol = sql.create_engine(conn_string_iol)
    connection_iol = engine_iol.connect()
    #RECUPERO INFO PER CREAZIONE PLOMINO DOCUMENT
    plominodb = psite.iol_cantieri
    plominodb.deleteDocuments()
    workflowTool = getToolByName(psite, "portal_workflow")
    mt = getToolByName(psite, 'portal_membership')
    
    #RECUPERO INFORMAZIONI SUI DATI DA IMPORTARE
    #DATI SU DB
    query = "SELECT * FROM istanze_cantieri.import_istanze order by numero DESC"
    res = connection.execute(query)
    cantieri = res.fetchall()
    result = list()
    i = 0
    #DATI DI WORKFLOW
    infoDB = getPlominoInfo()
    queryDel = "DELETE FROM istanze.occupazione_suolo"
    connection_iol.execute(queryDel)
    #CICLO SUI DOCUMENTI
    for r in cantieri:
        i+=1
        cantiere= dict(r)
        id = cantiere['id'].encode('ascii','ignore')
        print "Considero record %d con id %s " %(i,id)
        data = dict()
        data['elementi_scavo_dg'] = list()
        query = "SELECT 0 as tipologia_occupazione,'empty' as elemento_descrizione, lunghezza as occupazione_lunghezza , larghezza as occupazione_larghezza, zona as elemento_zona FROM istanze_cantieri.elementi WHERE istanza='%s'" %id
        res = connection.execute(query)
        elementi = res.fetchall()
        for el in elementi:
            data['elementi_scavo_dg'].append(dict(el))
        
        data['geometry'] = getGeometry(connection,id) or ''
        if  data['geometry'].find('POINT')>0:
            data['elemento_tipo'] = 'punto'
        elif data['geometry'].find('LINESTRING')>0:
            data['elemento_tipo'] = 'linea'
        
        for key,val in cantiere.iteritems():
            if key in cantieriDict.keys():
                data[cantieriDict[key]] = val
        data['search_richiedente'] = '%s %s %s' %(cantieri['cognome'],cantieri['nome'],cantieri['ragsoc'] or '')
        data['iol_tipo_app'] = 'cantieri'
        data['iol_tipo_richiesta'] = 'base'
        data['iol_tipo_pratica'] = 'cantieri_base'
        
        if id in infoDB.keys():
            info = infoDB[id]
            dataCantieri = dict(
                id = id,
                plominoform = 'frm_cantieri_base',
                plominodb = 'iol_cantieri',
                owner = data['owner'],
                url = None,
                review_state = info['review_state'],
                review_history = info['review_history'],
                iol_owner = [data['owner']],
                iol_reviewer = ['istruttori-cantieri'],
                iol_manager = ['rup-cantieri'],
                data = data
            )
           
            #data['Plomino_Authors']=info['data']['Plomino_Authors']
            #CREAZIONE DEL PLOMINO DOCUMENT
            
            doc = plominodb.createDocument(id.encode('ascii'))
            
            for key,val in info['data'].iteritems():
                if key not in data.keys():
                    if isinstance(val,datetime.datetime):
                        doc.setItem(key,DateTime.DateTime(val))
                    else:
                        if not key in ['Form']:
                            doc.setItem(key,val)
            doc.setItem('Form','frm_cantieri_base')
            # Setting Items on Document
            for key,val in data.iteritems():
                if key == 'elementi_scavo_dg':
                    res = list()
                    for v in val:
                        res.append([v['tipologia_occupazione'],v['elemento_descrizione'],v['occupazione_lunghezza'],v['occupazione_larghezza'],v['elemento_zona']])
                    doc.setItem(key,res)
                else:
                    if isinstance(val,datetime.datetime):
                        doc.setItem(key,DateTime.DateTime(val))
                    else:
                        if not key in ['istruttore']:
                            doc.setItem(key,val)
                    
            # Setting Ownership
            if data['owner']:
                member = mt.getMemberById(data['owner'])
                doc.changeOwnership(member, recursive=False)
            # Setting Roles
            doc.manage_setLocalRoles('istruttori-cantieri', ["iol-reviewer",])
            doc.manage_setLocalRoles('rup-cantieri', ["iol-manager",])
            #Apply Workflow to document
            wfInfo = info['review_history']
            for wf in wfInfo:
                
                if wf['action'] in mappingWF:
                    print "considero %s:" %wf['action']
                    for tr in mappingWF[wf['action']]:
                        print "\t ==> considero : %s" %tr
                        try:
                            workflowTool.doActionFor(doc, tr)
                            print "\t\t ==> Eseguita transizione %s su documento %s" %(tr,id)
                        except WorkflowException as e:
                            print "\t\t ==> Errore Transizione %s su documento %s : %s " %(tr,id,str(e))
            if cantiere['istruttore']:
                istruttore = cantiere['istruttore'].encode('ascii','ignore')
            else:
                istruttore = None
            doc.setItem('istruttore',istruttore)
            #SALVATAGGIO SU DATABASE
            try:
                saveData(dataCantieri,connection_iol)
                #print "Considerati %d cantieri" %i  
            except Exception as e:
                print 'Errore nel salvataggio del record %d : %s' %(i,str(e))

           
    return 1
if "app" in locals():
    # Use Zope application server user database (not plone site)
    admin=app.acl_users.getUserById("admin")
    
    newSecurityManager(None, admin)
    res = importAutorizzazioni(app)

    import transaction;
    transaction.commit()
    # Perform ZEO client synchronization (if running in clustered mode)
    app._p_jar.sync()