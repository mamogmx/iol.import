keysCantieri=['id','numero','username','istruttore','data_presentazione','protocollo','data_protocollo','oggetto','prescrizioni','note','stato_istanze','datainizio','datafine','orario','rinnovoinsanatoria','onerosa','ricevuta','cauzione','ricevutacauzione','svincolo','importo','cognome','nome','indirizzo','comune','prov','cap','telefono','email','comunato','provnato','datanato','sesso','codfis','ragsoc','titolod','sede','comuned','provd','capd','piva','convenzione','data_pagamento','datainizio_eff','datafine_eff','note_utente','motivo_sospensione','n_autorizzazione','data_autorizzazione']


valuesCantieri=['id','numero_pratica','owner','istruttore','data_pratica','numero_protocollo','data_protocollo','cantiere_motivazione','prescrizioni_istruttore','cantieri_nore','wf_iol','autorizzata_dal','autorizzata_al','cantieri_orario','rinnovo_sanatoria','onerosa','ricevuta','cauzione','ricevutacauzione','svincolo','importo','fisica_cognome','fisica_nome','fisica_indirizzo','fisica_comune','fisica_provincia','fisica_cap','fisica_telefono','fisica_email','fisica_comune_nato','fisica_provincia_nato','fisica_data_nato','fisica_sesso','fisica_cf','giuridica_denominazione','titolod','giuridica_indirizzo','giuridica_comune','giuridica_provincia','giuridica_cap','giuridica_piva','convenzione','data_pagamento','datainizio_eff','datafine_eff','note_utente','istruttoria_motivo_sospensione','numero_autorizzazione','data_autorizzazione']

cantieriDict=dict()
for i in range(len(valuesCantieri)):
   cantieriDict[keysCantieri[i]]=valuesCantieri[i]



keysElementiCantieri=['id','istanza','descrizione','zona','lunghezza','larghezza']

valuesElementiCantieri=['id','istanza','elemento_descrizione','elemento_zona','occupazione_lunghezza','occupazione_larghezza']

elementiDict = dict()
for elem in range(len(valuesElementiCantieri)):
    elementiDict[keysElementiCantieri[elem]]=valuesElementiCantieri[elem]


