import simplejson as json
import sqlalchemy as sql

import urllib
import os
import time

from Products.CMFPlomino.PlominoUtils import DateToString,Now,StringToDate
from Products.CMFCore.utils import getToolByName
from AccessControl.SecurityManagement import newSecurityManager
from mimetypes import MimeTypes
import dizImportazioneCantieri
conn_string = "postgres://postgres:postgres@192.168.1.133:5433/sitar"

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
        sql = "SELECT 0 as tipologia_occupazione,'empty' as elemento_descrizione, lunghezza as occupazione_lunghezza , larghezza as occupazione_larghezza, zona as elemento_zona FROM istanze_cantieri.elementi WHERE istanza='%s'" %r['id']
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