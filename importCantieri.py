import simplejson as json
import sqlalchemy as sql

import urllib
import os
import time

from Products.CMFPlomino.PlominoUtils import DateToString,Now,StringToDate
from Products.CMFCore.utils import getToolByName
from AccessControl.SecurityManagement import newSecurityManager
from mimetypes import MimeTypes

base_path = '/data/spezia/scavi/'
appUrl = 'http://apps.spezianet.it/scavi/'
docBaseUrl = '%splomino_documents/' %(appUrl)

if "app" in locals():
    # Use Zope application server user database (not plone site)
    admin=app.acl_users.getUserById("admin")
    newSecurityManager(None, admin)
    #Your code here

    #Transaction Commit 
    import transaction;
    transaction.commit()
    # Perform ZEO client synchronization (if running in clustered mode)
    app._p_jar.sync()
