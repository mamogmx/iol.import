import json
import urllib
import os
import time
base_path = '/data/spezia/scavi/'
appUrl = 'http://apps.spezianet.it/scavi/'
docBaseUrl = '%splomino_documents/' %(appUrl)
from Products.CMFPlomino.PlominoUtils import DateToString,Now,StringToDate
from Products.CMFCore.utils import getToolByName
from AccessControl.SecurityManagement import newSecurityManager
from mimetypes import MimeTypes

wfTranslation = dict(
    acc_osservazioni = ['integra'],
    assegnazione_ri = ['assegna'],
    autorizza_lavori = ['autorizza'],
    diniego = ['rigetta'],
    integrazione = ['integra'],
    integrazione_strade = ['integra'],
    istruttoria_ok =['istruttoria_completata'],
    istruttoria_ok_convenz = ['istruttoria_completata','autorizza'],
    preavviso_rigetto = ['preavviso_rigetto'],
    presenta_domanda = ['invia_domanda','protocolla'],
    sospensione = ['sospendi'],
    sospensione_strade = ['sospendi'],
    start_istruttore = ['invia_domanda','protocolla','assegna'],
)
elencoCampi = {'ragione_sociale': 'ragsoc_impresa', 'foto_sezione_1': 'foto_sezione_1', 'foto_sezione_2': 'foto_sezione_2', 'foto_sezione_3': 'foto_sezione_3', 'giuridica_comune': 'comuned', 'impresa_comune': 'comune_impresa', 'foto_il_3': 'foto_il_3', 'foto_il_2': 'foto_il_2', 'foto_il_1': 'foto_il_1', 'giuridica_piva': 'piva', 'data_protocollo_autorizzazione': 'data_prot_autorizzazione', 'impresa_provincia': 'prov_impresa', 'metri_asfalto': 'metri_asfalto', 'istruttoria_annotazioni': 'note', 'giuridica_email': 'emaild', 'numero_protocollo_autorizzazione': 'protocollo_autorizzazione', 'autorizzata_dal': 'datainizio', 'impresa_tel': 'tel_resp_cantiere', 'numero_protocollo': 'protocollo', 'data_protocollo': 'data_prot', 'giuridica_qualita': 'titolod', 'impresa_indirizzo': 'sede_impresa', 'fisica_cap': 'cap', 'giuridica_indirizzo': 'sede', 'fisica_data_nato': 'datanato', 'impresa_responsabile': 'resp_cantiere', 'giuridica_denominazione': 'ragsoc', 'diritto_fisso': 'diritto_fisso', 'istruttoria_motivo_sospensione': 'motivo_sospensione', 'giuridica_provincia': 'provd', 'istruttoria_strade_sospensione': 'sospensione_strade', 'fisica_nome': 'nome', 'fisica_email': 'email', 'numero_autorizzazione': 'n_autorizzazione', 'elemento_zona': 'zona', 'fisica_telefono': 'telefono', 'giuridica_cap': 'capd', 'cauzione': 'cauzione', 'istruttoria_strade_prescrizioni': 'prescrizioni_strade', 'elemento_geom': 'the_geom', 'elemento_lunghezza': 'lunghezza', 'metri_lastricato': 'metri_lastricato', 'documenti_autorizzazione': 'comunicazioni', 'scavi_note': 'note_utente', 'importo_cosap': 'importo_cosap', 'fisica_comune_nato': 'comunato', 'scavi_orario': 'orario', 'ricevuta_cauzione': 'ricevuta_cauzione', 'foto_fl_2': 'foto_fl_2', 'foto_fl_3': 'foto_fl_3', 'autorizzata_al': 'datafine', 'motivo_urgenza': 'motivo_urgenza', 'fisica_provincia_nato': 'provnato', 'istruttoria_prescrizioni': 'prescrizioni', 'fisica_cf': 'codfis', 'metri_marciapiede': 'metri_marciapiede', 'istruttore': 'istruttore', 'foto_fl_1': 'foto_fl_1', 'data_pratica': 'data_presentazione', 'elemento_larghezza': 'larghezza', 'foto_riempimento_2': 'foto_riempimento_2', 'foto_riempimento_3': 'foto_riempimento_3', 'elemento_descrizione': 'descrizione', 'foto_riempimento_1': 'foto_riempimento_1', 'datafine_eff': 'datafine_eff', 'fisica_indirizzo': 'indirizzo', 'fisica_provincia': 'prov', 'data_pagamento_cosap': 'data_pagamento', 'elemento_tipo': 'tipo', 'data_cauzione': 'data_cauzione', 'impresa_cap': 'cap_impresa', 'datainizio_eff': 'datainizio_eff', 'data_autorizzazione': 'data_autorizzazione', 'cantiere_motivazione': 'oggetto', 'ricevuta_cosap': 'ricevuta_cosap', 'istruttoria_strade_annotazione': 'note_strade', 'allegato_pagamento': 'allegato_pagamento', 'numero_pratica': 'numero', 'fisica_sesso': 'sesso', 'fisica_cognome': 'cognome', 'fisica_comune': 'comune', 'elemento_profondita': 'profondita', 'impresa_piva': 'piva_impresa'}
elencoCampiElementi = dict(
    parentdoc = 'istanza',
    elemento_descrizione = 'descrizione',
    elemento_zona = 'zona',
    elemento_lunghezza = 'lunghezza',
    elemento_larghezza = 'larghezza',
    elemento_profondita = 'profondita',
    elemento_geometry  = 'the_geom'
)

def getPlominoId():
    f = urllib.urlopen('http://apps.spezianet.it/scavi/resources/pippo')
    data = f.read()
    f.close()
    out_file = open("data.json","w")
    out_file.write(data)
    out_file.close()


def getPlominoData(id):
    exportUrl = '%sresources/exportScaviItems?docId=%s' %(appUrl,id)
    f = urllib.urlopen(exportUrl)
    data = f.read()
    if data and data[0]=='{':
        data = json.loads(data)
    else:
        err = 'no data in %s' %exportUrl
        return (None,None,None,err)
    f.close()

    if 'elementi' in data:
        elementi = data['elementi']
        del data['elementi']
    else:
        elementi = None
    if('wf' in data):
        wf = data['wf']
        del data['wf']
        return (wf , data,elementi,None)
    else:
        return (None , data,elementi,None)


def getAllegati():
    f = open("data.json",'r')
    info = json.loads(f.read())
    f.close()
    cont = 0
    for el in info:
        cont+=1
        docPath = '%s%s' %(base_path,el['id'])
        if not os.path.isdir(docPath):
            os.mkdir(docPath)
            print "%d) Directory %s creata" %(cont,docPath)
        for fName in el['allegati']:
            if not os.path.isfile("%s/%s" %(docPath,fName)):
                f = urllib.urlopen('%sresources/exportScavi?docId=%s&fName=%s' %(appUrl,el['id'],fName))
                data = f.read()
                f.close()
                out_file = open("%s/%s" %(docPath,fName),"w")
                out_file.write(data)
                out_file.close()
                print "\t File %s scaricato" %fName

    return 1

def getMimeType(path):
    mime = MimeTypes()
    url = urllib.pathname2url(path)
    mime_type = mime.guess_type(url)
    return  mime_type
def esportaScavi(app):
    psite = app.unrestrictedTraverse("istanze")
    psite.manage_exportObject(id='iol_scavi')

def setStatus(app):
    psite = app.unrestrictedTraverse("istanze")
    db = psite.iol_scavi
    idx = db.getIndex()
    cont = 0
    start = time.time()
    for br in idx.dbsearch({'Form':'frm_scavi_base'}):
        doc = br.getObject()
        doc.setItem('wf_iol', doc.wf_getInfoFor('review_state'))
        idx.indexDocument(doc)
        if (cont % 100)==0:
            end=time.time()
            print "Indicizzati %d documenti in %d secondi...." %(cont,(end - start))
            start = end
        cont += 1

def setElementiForm(app):
    cont = 0
    psite = app.unrestrictedTraverse("istanze")
    pw = getToolByName(psite,'portal_workflow')
    db = psite.iol_scavi
    fields = dict(
        elementi_descrizione = 'elemento_descrizione',
        elementi_zona = 'elemento_zona',
        elementi_lunghezza = 'elemento_lunghezza',
        elementi_larghezza = 'elemento_larghezza',
        elementi_profondita = 'elemento_profondita',
        elementi_geometry  = 'the_geom'
    )
    for doc in db.getAllDocuments():
        if  doc.getItem('Form','')=='frm_elemento':
            doc.removeItem('parentdoc')
            for k,v in fields.items():
                val = doc.getItem(k)
                doc.setItem(v,val)
                doc.removeItem(k)
def importaScavi(app):
    cont = 0
    psite = app.unrestrictedTraverse("istanze")
    pw = getToolByName(psite,'portal_workflow')
    db = psite.iol_scavi
    db.deleteDocuments( ids=None, massive=True)

    basePath = '/data/spezia/scavi/'
    frmName = 'frm_scavi_base'
    fields = elencoCampi
    f = open("data.json",'r')
    info = json.loads(f.read())
    f.close()
    cont = 0
    errors = dict()
    for el in info:
        error=list()
        tic = time.time()
        cont+=1
        id=str(el['id'])
        #elementi = el['elementi']
        allegati = el['allegati']
        doc = db.createDocument(docid=id)
        print "%d) Creato documento %s" %(cont,id)
        doc.setItem('Form',frmName)
        start = time.time()
        wfInfo ,rec,elementi,err = getPlominoData(id)

        if err:
            message = "Errore nel recupero delle informazioni: %s" %err
            print "\t%s" %message
            error.append(message)

            continue
        end=time.time()

        print "\tRecuperate informazioni del documento %s in %s secondi" %(id,str(end-start))
        start = end
        #Importazione dei campi
        for field,value in fields.items():
            if value and rec  and (value in rec):
                vv = rec[value]['value']
                if rec[value]['type']=='date':
                    v = StringToDate(vv,'%d/%m/%Y %H:%M:%S')
                else:
                    v = vv
                doc.setItem(field,v)

        doc.setItem('search_richiedenti',' %s %s' %(rec['cognome']['value'],rec['nome']['value']))
        doc.setItem('iol_tipo_app','scavi')
        doc.setItem('iol_tipo_richiesta','base')
        doc.setItem('Plomino_Authors',['admin','mamo'])
        end=time.time()
        print "\tSettate informazioni del documento %s in %s secondi" %(id,str(end-start))

        docPath = '%s/%s/' %(basePath,id)
        #Importazione dei documenti
        start = end

        for fName in allegati:
            testo = ''
            try:
                f = open("%s%s" %(docPath,fName),'r')
                testo = f.read()
                f.close
            except Exception as e:
                message = "Errore nell'allegato %s del documento %s : %s" %(fName,id,str(e))
                print "\t%s" %message
                error.append(message)
            if testo:
                mime = getMimeType("%s%s" %(docPath,fName))
                name,content = doc.setfile(submittedValue=testo,filename=fName,overwrite=True,contenttype=mime)
        end=time.time()
        print "\tImportati gli allegati del documento %s in %s secondi" %(id,str(end-start))
        start = end
        #Importazione del workflow

        wfCurr = pw.getWorkflowsFor(doc)[0]
        for wfObj in wfInfo:
            if wfObj['action'] in wfTranslation:
                for wfAction in wfTranslation[wfObj['action']]:
                    if wfAction and wfCurr.isActionSupported(doc,wfAction):
                        try:
                            wfCurr.doActionFor(doc,wfAction,comment='',actor=wfObj['actor'],time=StringToDate(wfObj['time'],'%d/%m/%Y %H:%M:%S'))
                            print "\tEseguita Transizione %s" %wfAction
                        except:
                            message = "Transizione %s non disponibile" %wfAction
                            print "\t%s" %message
                            error.append(message)
            else:
                print "\t Transizionione %s non implementata" %wfObj['action']
        end=time.time()
        print "\tImportazione Workflow del documento %s in %s secondi" %(id,str(end-start))
        #Importazione degli elementi di scavo
        for e in elementi:
            idEl = str(e['id'])
            elem = db.createDocument(docid=idEl)

            for field,value in elencoCampiElementi.items():
                try:
                    vv = e[value]['value']
                    if e[value]['type']=='date':
                        v = StringToDate(vv,'%d/%m/%Y %H:%M:%S')
                    else:
                        v = vv
                except:
                    v = None
                elem.setItem(field,v)
        end=time.time()
        print "\tImportati gli elementi del documento %s in %s secondi" %(id,str(end-start))
        toc = time.time()
        print '%d) Pratica numero %s %s secondi' %(cont,str(rec['numero']['value']),toc-tic)
        if error:
            errors[id]=error
    out_file = open("errori-scavi.json","w")
    out_file.write(json.dumps(errors))
    out_file.close()
    return 1


if "app" in locals():
    # Use Zope application server user database (not plone site)
    admin=app.acl_users.getUserById("admin")
    newSecurityManager(None, admin)
    res = setStatus(app)
    print res
    import transaction;
    transaction.commit()
    # Perform ZEO client synchronization (if running in clustered mode)
    app._p_jar.sync()