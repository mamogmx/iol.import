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


conn_string_iol = "postgres://postgres:postgres@192.168.1.134:5434/sitar"

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
       "NumeroOmbrelloni" as testo_ombrelloni, "Numerotavoli" as testo_tavoli, "TotaleMq" as totalemq,case when(not "Pedana" is null) then 'si' else 'no' end as pedana_opt , "Pedana",case when ("Numerotavoli" ilike '%rampa%') then 'si' else 'no' end as rampa_opt
  FROM dehor_elementi),
  intestazioni as (SELECT "IDPratica" as idpratica,
        regexp_replace(replace(replace(replace("NomeComposto",' AU',''), ' ED',''),' PE',''), '[1*]+', '' ) as search_richiedente,split_part(regexp_replace(replace(replace(replace("NomeComposto",' AU',''), ' ED',''),' PE',''), '[1*]+', '' ),' ',1) as fisica_cognome,
        replace(regexp_replace(replace(replace(replace("NomeComposto",' AU',''), ' ED',''),' PE',''), '[1*]+', '' ),split_part(regexp_replace(replace(replace(replace("NomeComposto",' AU',''), ' ED',''),' PE',''), '[1*]+', '' ),' ',1)||' ','') as fisica_nome,"Indirizzo" as fisica_indirizzo, "NrCivico" as fisica_civico, "CAP" as fisica_cap, "Tel" as fisica_telefono,
       "CodFiscale" as fisica_cf, case when("Sesso"='M') then 'Maschile' else 'Femminile' end as fisica_sesso, "DataNascita"::timestamp as fisica_data_nato , "Comune" as fisica_comune_nato, "TelCellulare" as fisica_cellulare
  FROM dehor_intesta),
  dati as (SELECT "IDPratica" as idpratica, "IndirizzoOccupazione" as via,
       "CivicoOccupazione" as civico, "DataLicenza"::timestamp as data_licenza, "NumeroLicenza" as numero_licenza, "QualitaRichiedente" as fisica_titolo,
       "TipoLicenza" as tipo_licenza, "DallaData"::timestamp as autorizzata_dal, "AllaData"::timestamp as autorizzata_al, "SupLocali" as superficie_interna, "PeriodoOccupazione"
  FROM dehor_dati),
  geom as (SELECT idpratica,array_agg('SRID=4326;'||st_astext(st_transform(the_geom,4326))) as geometry from dehor_geom group by idpratica)

select * from attivita inner join autorizzazioni using(idpratica) inner join dati using(idpratica) inner join elementi using(idpratica) inner join intestazioni using(idpratica) inner join geom using(idpratica);
"""
    engine_iol = sql.create_engine(conn_string_iol)
    connection_iol = engine_iol.connect()
    res = connection_iol.execute(query)
    dehors = res.fetchall()
    return dehors

def populateDB(app):
    psite = app.unrestrictedTraverse("istanze")
    r = getData()
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