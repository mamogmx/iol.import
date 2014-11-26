import simplejson as json
import urllib
import os
import time
from Products.CMFPlomino.PlominoUtils import DateToString,Now,StringToDate
from Products.CMFCore.utils import getToolByName
from AccessControl.SecurityManagement import newSecurityManager
import sqlalchemy as sql
import decimal

def importCantieri(app):
    pass

if "app" in locals():
    # Use Zope application server user database (not plone site)
    admin = app.acl_users.getUserById("admin")
    newSecurityManager(None, admin)
    res = importCantieri(app)

# Commit transaction
import transaction
transaction.commit()
# Perform ZEO client synchronization (if running in clustered mode)
app._p_jar.sync()