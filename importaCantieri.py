import simplejson as json
import sqlalchemy as sql

import urllib
import os
import time

from Products.CMFPlomino.PlominoUtils import DateToString,Now,StringToDate
from Products.CMFCore.utils import getToolByName
from AccessControl.SecurityManagement import newSecurityManager
from mimetypes import MimeTypes
conn_string = "postgres://postgres:postgres@192.168.1.133:5433/sitar"

keysCantieri=['id','numero','username','istruttore','data_presentazione','protocollo','data_protocollo','oggetto','prescrizioni','note','stato_istanze','datainizio','datafine','orario','rinnovoinsanatoria','onerosa','ricevuta','cauzione','ricevutacauzione','svincolo','importo','cognome','nome','indirizzo','comune','prov','cap','telefono','email','comunato','provnato','datanato','sesso','codfis','ragsoc','titolod','sede','comuned','provd','capd','piva','convenzione','data_pagamento','datainizio_eff','datafine_eff','note_utente','motivo_sospensione','n_autorizzazione','data_autorizzazione']
valuesCantieri=['id','numero_pratica','owner','istruttore','data_pratica','numero_protocollo','data_protocollo','cantiere_motivazione','prescrizioni_istruttore','cantieri_nore','wf_iol','autorizzata_dal','autorizzata_al','cantieri_orario','rinnovo_sanatoria','onerosa','ricevuta','cauzione','ricevutacauzione','svincolo','importo','fisica_cognome','fisica_nome','fisica_indirizzo','fisica_comune','fisica_provincia','fisica_cap','fisica_telefono','fisica_email','fisica_comune_nato','fisica_provincia_nato','fisica_data_nato','fisica_sesso','fisica_cf','giuridica_denominazione','titolod','giuridica_indirizzo','giuridica_comune','giuridica_provincia','giuridica_cap','giuridica_piva','convenzione','data_pagamento','datainizio_eff','datafine_eff','note_utente','istruttoria_motivo_sospensione','numero_autorizzazione','data_autorizzazione']

cantieriDict=dict()
for i in range(len(valuesCantieri)):
   cantieriDict[keysCantieri[i]]=valuesCantieri[i]

def findData(app):
    psite = app.unrestrictedTraverse("istanze")
    engine = sql.create_engine(conn_string)
    connection = engine.connect()
    query = "SELECT * FROM istanze_cantieri.istanze"
    res = connection.execute(query)
    cantieri = res.fetchAll()
    result = list()
    for r in cantieri:
        data = dict()
        data['elementi_scavo_dg'] = list()
        query = "SELECT 0 as tipologia_occupazione,'empty' as elemento_descrizione, lunghezza as occupazione_lunghezza , larghezza as occupazione_larghezza, zona as elemento_zona FROM istanze_cantieri.elementi WHERE istanza='%s'" %r['id']
        res = connection.execute(query)
        elementi = res.fetchAll()
        for el in elementi.values():
            data['elementi_scavo_dg'].append(el)
        for key,val in r.iteritems():
            data[cantieriDict[key]] = val
        result.append(data)
    return result
if "app" in locals():
    # Use Zope application server user database (not plone site)
    admin=app.acl_users.getUserById("admin")
    newSecurityManager(None, admin)
    res = findData(app)
    print res[0]
    import transaction;
    transaction.commit()
    # Perform ZEO client synchronization (if running in clustered mode)
    app._p_jar.sync()