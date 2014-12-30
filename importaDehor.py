import simplejson as json
import sqlalchemy as sql
import sqlalchemy.orm as orm
import DateTime
import urllib
import os
import re
import datetime

from Products.CMFPlomino.PlominoUtils import DateToString,Now,StringToDate
from Products.CMFCore.utils import getToolByName
from AccessControl.SecurityManagement import newSecurityManager
from mimetypes import MimeTypes
from plone import api
from Products.CMFCore.WorkflowCore import WorkflowException
from pgReplication import *

doc_base_path = '/var/backups/dehor_doc/dehor_pdf/'
conn_string_iol = "postgres://postgres:postgres@10.95.10.27/sitar"
engine_iol = sql.create_engine(conn_string_iol)
connection_iol = engine_iol.connect()


def getDocuments(doc):
    id = doc.getId()
    path = "%s" %(doc_base_path)
    idpratica = str(doc.getItem('idpratica',''))
    if os.path.exists(path):
        listDocs = [ f for f in os.listdir(path) if f.startswith('%s_' %idpratica)]
        tot = len(listDocs)
        i = 0
        for name in listDocs:

            i += 1
            f = open("%s/%s" %(path,name),'r')
            text = f.read()
            f.close()
            doc.setfile(text,filename=name,contenttype='application/pdf')

        print "\tRecuperati %s file su %s" %(i,tot)
    else:
        print "\t Path %s non trovato" %path
    return 1
        
def getData():
    query = """
with attivita as (SELECT "IDPratica" as idpratica, "Numero" as numero_pratica, "Protocollo" as numero_protocollo, "DataRegistrazione"::timestamp as data_pratica, "DataAvvio"::timestamp as data_protocollo,
       "NomeComposto" as giuridica_denominazione, "Indirizzo" as giuridica_indirizzo, "NrCivico" giuridica_civico, 'La Spezia'::varchar as giuridica_comune,'SP'::varchar as giuridica_provincia,"CAP" as giuridica_cap, "Tel" as giuridica_telefono,
       "Piva" as  giuridica_piva, "TelCellulare" as  giuridica_cellulare
  FROM dehor_attivita),
  autorizzazioni as (SELECT "IDPratica" as idpratica, "DataAutorizzazione" as data_autorizzazione, "NAutorizzazione" as label_autorizzazione, replace(split_part(coalesce("NAutorizzazione",'OSD0/2014'),'/',1),'OSD','')::integer as numero_autorizzazione,
       "CondizioniVincolanti" as istruttoria_prescizioni,  case when ("AutPaes"='0') then 'no' else 'si' end as aut_paes, case when("NotaDir"='0') then 'no' else 'si' end as nota_dir,
       case when("ParereComm"='0') then 'no' else 'si' end as parere_comm, case when("ParereMob"='0') then 'no' else 'si' end as parere_mob, case when("ParereProtCiv"='0') then 'no' else 'si' end as parere_pc, case when("OrarioNotturno"='0') then 'no' else 'si' end as orario_notturno,
       case when("AlleggerimentoStruttura"='0') then 'no' else 'si' end as alleggerimento_struttura, case when("Allerta2"='0') then 'no' else 'si' end as allerta2
  FROM dehor_autorizzazione),
  elementi as (SELECT "IDPratica" as idpratica, "DimensioneOccupazione" as dim_occupazione, case when(not "NumeroBarriere" is null) then 'si' else 'no' end as antivento_opt,"NumeroBarriere" as testo_barriere,  "NumeroFioriere" as testo_fioriere,
       "NumeroOmbrelloni" as testo_ombrelloni, "Numerotavoli" as testo_tavoli, "TotaleMq" as totalemq,case when(not "Pedana" is null) then 'si' else 'no' end as pedana_opt , "Pedana" as testo_pedana,case when ("Numerotavoli" ilike '%%rampa%%') then 'si' else 'no' end as rampa_opt
  FROM dehor_elementi),
  intestazioni as (SELECT "IDPratica" as idpratica,
        regexp_replace(replace(replace(replace("NomeComposto",' AU',''), ' ED',''),' PE',''), '[1*]+', '' ) as search_richiedente,split_part(regexp_replace(replace(replace(replace("NomeComposto",' AU',''), ' ED',''),' PE',''), '[1*]+', '' ),' ',1) as fisica_cognome,
        replace(regexp_replace(replace(replace(replace("NomeComposto",' AU',''), ' ED',''),' PE',''), '[1*]+', '' ),split_part(regexp_replace(replace(replace(replace("NomeComposto",' AU',''), ' ED',''),' PE',''), '[1*]+', '' ),' ',1)||' ','') as fisica_nome,"Indirizzo" as fisica_indirizzo, "NrCivico" as fisica_civico, "CAP" as fisica_cap, "Tel" as fisica_telefono,
       "CodFiscale" as fisica_cf, case when("Sesso"='M') then 'Maschile' else 'Femminile' end as fisica_sesso, "DataNascita"::timestamp as fisica_data_nato , "Comune" as fisica_comune_nato, "TelCellulare" as fisica_cellulare
  FROM dehor_intesta),
  dati as (SELECT "IDPratica" as idpratica, "IndirizzoOccupazione" as via,
       "CivicoOccupazione" as civico, "DataLicenza"::timestamp as data_licenza, "NumeroLicenza" as numero_licenza, "QualitaRichiedente" as fisica_titolo,
       "TipoLicenza" as tipo_licenza, "DallaData"::timestamp as autorizzata_dal, "AllaData"::timestamp as autorizzata_al, "SupLocali" as superficie_interna, case when ("PeriodoOccupazione"='Permante') then 'annuale' else 'temporanea' end as durata_occupazione
  FROM dehor_dati),
  geom as (SELECT idpratica,array_agg('SRID=4326;'||st_astext(st_transform(the_geom,4326))) as geometry_list from dehor_geom group by idpratica),
  pagamenti as (SELECT "IDPratica" as idpratica,coalesce(dovuto,0) as dovuto,coalesce(pagato,-1) as pagato FROM dehor_pagamenti)

select * from attivita inner join autorizzazioni using(idpratica) inner join dati using(idpratica) inner join elementi using(idpratica) inner join intestazioni using(idpratica) inner join geom using(idpratica) left join pagamenti using(idpratica);
"""
    
    res = connection_iol.execute(query)
    dehors = res.fetchall()
    return dehors

def populateDB(app):
    psite = app.unrestrictedTraverse("istanze")
    plominodb = psite.iol_dehor
    
    plominodb.deleteDocuments()
    workflowTool = getToolByName(psite, "portal_workflow")
    mt = getToolByName(psite, 'portal_membership')
    pg = getToolByName(psite, 'portal_groups')
    owner = pg.getGroupById('istruttori-dehor')
    owner = pg.getUserById('mamo')
    result = list()
    i = 1
    res = getData()
    tot = len(res)
    print "Letti %d Dehor" %tot
    for r in res:
        dehor = dict(r)
        doc = plominodb.createDocument()
        id = doc.getId()
        print "%d)Preso in considerazione il documento %s" %(i,id)
        doc.setItem('iol_tipo_app','dehor')
        doc.setItem('iol_tipo_pratica','dehor_base')
        doc.setItem('iol_tipo_richiesta','base')
        doc.setItem('Form','frm_vuoto')
        try:
            for key,val in dehor.iteritems():
                if key in ('geometry_list'):
                    doc.setItem('geometry',val[0])
                    doc.setItem(key,val)
                elif key in ('dovuto','pagato'):
                    doc.setItem('pagamenti_saldo',-1 * ((dehor['dovuto'] or 0) - (dehor['pagato'] or -1)))
                    doc.setItem(key,val)

                elif key in ('search_richiedente'):
                    doc.setItem(key,"%s %s" %(val,dehor['giuridica_denominazione']))
                else:
                    if isinstance(val,datetime.datetime):
                        doc.setItem(key,DateTime.DateTime(val))
                    else:
                        doc.setItem(key,val)
        except Exception as e:
            print "\t Errore nel salvataggio dei dati del documento %s" %id
            print str(e)

        getDocuments(doc)

        try:
            doc.changeOwnership(owner, recursive=False)
            # Setting Roles
            doc.manage_setLocalRoles('istruttori-dehor', ["iol-reviewer",])
            doc.manage_setLocalRoles('rup-dehor', ["iol-manager",])
            print "\t Assegnati Ruoli Correttamente"
        except Exception as e:
            print "\t Errore nel salvataggio dei ruoli del documento %s" %id
            print str(e)
        try:
            workflowTool.doActionFor(doc, 'autorizza_importazione')
            print "\t Transizione 'autorizza_importazione effettuata correttamente'"
        except Exception as e:
            print "\t Errore nell'esecuzione del workflow sul documento %s" %id         
            print str(e)
        
        try:
            doc.setItem('Form','frm_dehor_base')
            saveData(doc,connection_iol,'istanze_test','occupazione_suolo',workflowTool)
            
        except Exception as e:
            print "\t Errore nel salvataggio del documento %s" %id
            print str(e)
        i += 1
    return r

if "app" in locals():
    # Use Zope application server user database (not plone site)
    admin=app.acl_users.getUserById("admin")

    newSecurityManager(None, admin)
    res = populateDB(app)

    import transaction;
    transaction.commit()
    # Perform ZEO client synchronization (if running in clustered mode)
    app._p_jar.sync()