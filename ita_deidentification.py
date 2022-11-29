'''
De-identification code for Italian Clinical Text

'''

# import datefinder # to install with pip
# install Stanza with pip
import pandas as pd
import re
import dateutil.parser
# from typing import Match
from functools import reduce
import json
import os

import sparknlp
import sparknlp_jsl
from sparknlp.annotator import *
from sparknlp_jsl.annotator import *
from sparknlp.base import *
from sparknlp.util import *


tc = '■' # temporary character for replacement

def empty_db():
  return pd.DataFrame(columns=['start','end','entity_type','text'])

class anonymizer:
  def __init__(self, configfile):
    # parse configuration file
    with open(configfile) as cin:
      cfg = json.load(cin)

    self.models = cfg['models']
    self.tracker = pd.DataFrame(self.models.items(),columns=['entity_type','model'])
    self.tracker['status'] = False # status to avoid rerunning the same ner models multiple times
    self.mode = cfg['mask']['mode']
    self.sc = cfg['mask']['special_character'][0] # take only first character if a string is passed
    self.date_level = cfg['mask']['date_level']
    self.mask_modes = ['tag','tag_l','anon','anon_l']

    # results dataframe
    self.dbs = empty_db()

    # initialize needed models
    print(f'{"DOWNLOADING AND INITIALIZING MODELS ":-<80}')
    for model in set(self.models.values()):
      if model=='stanza':
        import stanza # to install with pip
        stanza.download("it")
        self.m_stanza = stanza.Pipeline(lang="it", processors='tokenize, ner')
        #todo: needs to check if stanza is trying to be used on other entities which are not supported
      elif model=='spacy':
       import spacy #to install with pip
       # Install 'python -m spacy download it_core_news_lg'
       self.m_spacy = spacy.load("it_core_news_lg")
      elif model=='regex':
        # All the regex refers to Italian Style for telephone, zip code, etc ---
        pass # nothing to load
      elif model=='john':
        from sparknlp.pretrained import PretrainedPipeline
        from pyspark.ml import Pipeline, PipelineModel
        from pyspark.sql import SparkSession

        with open('spark_jsl.json') as f:
          license_keys = json.load(f)

        # Defining license key-value pairs as local variables
        locals().update(license_keys)
        os.environ.update(license_keys)

        params = {"spark.driver.memory": "16G",
                  "spark.kryoserializer.buffer.max": "2000M",
                  "spark.driver.maxResultSize": "2000M"}

        from sparknlp.pretrained import ResourceDownloader
        from pyspark.sql import functions as F
        spark = sparknlp_jsl.start(secret=os.environ.get('SECRET'), params=params)
        self.m_john = PretrainedPipeline("clinical_deidentification", "it", "clinical/models")
      elif model!='':
        print(f'{model} is not a supported model. Please check the documentation for the list of supported models for each field')

  def reload(self, config):
    #todo: needs to update on reload only if models are different
    return

  # main function
  def deIdentificationIta(self, inputText):
    #print(f'{"ANONYMIZING GIVEN TEXT ":-<80}')
    self.dbs = empty_db() # resets previously found entities

    # decide if the returned dataframes can be useful. careful if using same ner for multiple ents, some dbs have all the ents and others are empty to avoid rerunning
    _ = self.FindTelephone(inputText, concat=True)
    _ = self.FindZipCode(inputText, concat=True)
    _ = self.FindEmail(inputText, concat=True)
    _ = self.FindPerson(inputText, concat=True)
    _ = self.FindOrganization(inputText, concat=True)
    _ = self.FindAddress(inputText, concat=True)
    _ = self.FindDate(inputText, concat=True)
    _ = self.FindFiscalCode(inputText, concat=True)

    self.tracker['status'] = False # resets found entities for future reruns on different text
    outText = self.mask_data(inputText)
    return {'text': outText, 'match_dataframe': self.dbs}

  # masker function. replace sensible text with tag name (optional) and special character
  def mask_data(self, inputText, dbs=None, mode=None, date_level=None, sc=None):
    # use class variables as default but lets the user call just mask_data if no Finds are needed
    if dbs is None: dbs=self.dbs
    if mode is None: mode=self.mode
    if date_level is None: date_level=self.date_level
    if sc is None: sc=self.sc

    if mode not in self.mask_modes:
      print(f'WARNING: Unsupported masking mode selected. Choose one from {self.mask_modes}.')
      return inputText
    if len(dbs)==0:
      return inputText

    # makes a unique span dataframe taking only first span in case of overlaps
    # il primo reset index è per evitare indici duplicati ma mantenere l'ordine di preferenza (nella lista i db prima verranno messi prima come priorità), il secondo è per resettare dopo il sort
    dbtot = dbs.reset_index(drop=True).sort_values(by=['start','end']).reset_index(drop=True)
    dbase = dbtot[dbtot.index==0].copy()
    db2add = dbtot[dbtot.index!=0]
    for i,row in db2add.iterrows():
      if row['start']>dbase['end'].max():
        dbase = pd.concat((dbase,db2add[db2add.index==i]))
    dbase = dbase.reset_index(drop=True)

    outText = ''
    for i in range(len(dbase)):
      if i==0:
        p_end = 0 # for first iteration get string start
      else:
        p_end = dbase.loc[i-1]['end'] # end of previous span
      row = dbase.loc[i]
      field_len = row['end']-row['start'] # length of text to anonymize
      outText += inputText[p_end:row['start']] # concats normal text between spans

      if row['entity_type']=='DATA' and date_level!='hide': # additional types of anonymization for dates
        tuple_months = ('gennaio','01'),('gen','01'),('febbraio','02'),('feb','02'),('marzo','03'),('mar','03'),('aprile','04'),('apr','04'),('maggio','05'),('mag','05'),('giugno','06'),('giu','06'),('luglio','07'),('lug','07'),('agosto','08'),('ago','08'),('settembre','09'),('set','09'),('ottobre','10'),('ott','10'),('novembre','11'),('nov','11'),('dicembre','12'),('dic','12')
        data_to_hide = row['text']
        if(not(re.search('[a-zA-Z]', data_to_hide)) == None):
          data_to_hide = reduce(lambda a, kv: a.replace(*kv), tuple_months, data_to_hide) # replaces textual month with numeric equivalent
        try:
          data_finder = dateutil.parser.parse(data_to_hide, dayfirst=True)
        except dateutil.parser.ParserError as e:
          print('WARNING: Date parsing error: ', e, '... Ignoring the match ', data_to_hide)
          continue
        if date_level == 'year':
          outText += f'{data_finder.year}'
        elif date_level == 'month':
          outText += f'{data_finder.month}-{data_finder.year}'
        else: # this should never occur cause of the check in FindDate
          outText += data_to_hide

      else: # non-date entity_types, or dates with "hide" level behaving as other entities
        if mode=='anon_l' or mode=='anon':
          outText += tc*(field_len)
        elif mode=='tag_l' or mode=='tag':
          outText += f'{"<"+row["entity_type"]+">":{tc}<{field_len}}' # also puts angular brackets around entity_type
          # !!! NB: if entity_type string is longer than text string, overall text length will increase
        #todo: elif self.mode=='random'
    outText += inputText[dbase.iloc[-1]['end']:] # concat last piece of text after last span

    if mode=='anon':
      outText = re.sub(f'{tc}+', tc*3, outText) # you can set how long the fixed character replacement will be here
    if mode=='tag':
      outText = outText.replace(tc,'')
    outText = outText.replace(tc,sc)
    return outText

  # entity finder functions
  # concat is false by default meaning results won't be appended to object dbs, letting you call individual functions when needed
  def FindTelephone(self, inputText, concat=False):
    if self.models['telephone']=='regex':
      matches = re.finditer('(?:((\+?\(?\d{2,3}\)?)|(\(\+?\d{2,3}\))) ?)?(((\d{2}[\ \-\.\/]?){3,5}\d{2})|((\d{3}[\ \-\.\/]?){2}\d{4}))', inputText)
      span_list = [(match.span()[0], match.span()[1], match.group()) for match in matches]
      db = pd.DataFrame(span_list, columns=['start','end','text'])
      db['entity_type'] = 'TELEFONO'
      if concat: self.dbs = pd.concat((self.dbs, db))
      self.tracker.loc[self.tracker['entity_type']=='telephone','status']=True
      return db
    elif self.models['telephone']=='john':
      return self.Find_with_John(inputText, concat)
    elif self.models['telephone']=='':
      return empty_db()
    else:
      print('WARNING: Unsupported model for telephone anonymization')
      return empty_db()

  def FindZipCode(self, inputText, concat=False):
    if self.models['zipcode']=='regex':
      matches = re.finditer('\D([0-9]{5})\D', inputText)
      span_list = [(match.span(1)[0], match.span(1)[1], match.group(1)) for match in matches] # (1) because it's first capturing group to avoid surrounding non-digits captured
      db = pd.DataFrame(span_list, columns=['start','end','text'])
      db['entity_type'] = 'CAP'
      if concat: self.dbs = pd.concat((self.dbs, db))
      self.tracker.loc[self.tracker['entity_type']=='zipcode','status']=True
      return db
    elif self.models['zipcode']=='':
      return empty_db()
    elif self.models['zipcode']=='john':
      return self.Find_with_John(inputText, concat)
    else:
      print('WARNING: Unsupported model for zipcode anonymization')
      return empty_db()

  def FindEmail(self, inputText, concat=False):
    if self.models['email']=='regex':
      matches = re.finditer('[^\s@]+@([^\s@.,]+\.)+[^\s@.,]{2,}', inputText)
      span_list = [(match.span()[0], match.span()[1], match.group()) for match in matches]
      db = pd.DataFrame(span_list, columns=['start','end','text'])
      db['entity_type'] = 'E-MAIL'
      if concat: self.dbs = pd.concat((self.dbs, db))
      self.tracker.loc[self.tracker['entity_type']=='email','status']=True
      return db
    elif self.models['email']=='':
      return empty_db()
    elif self.models['email']=='john':
      return self.Find_with_John(inputText, concat)
    else:
      print('WARNING: Unsupported model for e-mail anonymization')
      return empty_db()

  def FindFiscalCode(self, inputText, concat=False):
    if self.models['fiscal_code']=='regex':
      matches = re.finditer('[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]', inputText)
      span_list = [(match.span()[0], match.span()[1], match.group()) for match in matches]
      db = pd.DataFrame(span_list, columns=['start','end','text'])
      db['entity_type'] = 'CF'
      if concat: self.dbs = pd.concat((self.dbs, db))
      self.tracker.loc[self.tracker['entity_type']=='fiscal_code','status']=True
      return db
    elif self.models['fiscal_code']=='':
      return empty_db()
    elif self.models['fiscal_code']=='john':
      return self.Find_with_John(inputText, concat)
    else:
      print('WARNING: Unsupported model for fiscal code anonymization')
      return empty_db()

  def FindPerson(self, inputText, concat=False):
    if self.models['person']=='stanza':
      return self.Find_with_Stanza(inputText, concat)
    elif self.models['person']=='spacy':
      return self.Find_with_Spacy(inputText, concat)
    elif self.models['person']=='':
      return empty_db()
    elif self.models['person']=='john':
      return self.Find_with_John(inputText, concat)
    else:
      print('WARNING: Unsupported model for person anonymization')
      return empty_db()

  def FindOrganization(self, inputText, concat=False):
    if self.models['organization']=='stanza':
      return self.Find_with_Stanza(inputText, concat)
    elif self.models['organization']=='spacy':
      return self.Find_with_Spacy(inputText, concat)
    elif self.models['organization']=='':
      return empty_db()
    elif self.models['organization']=='john':
      return self.Find_with_John(inputText, concat)
    else:
      print('WARNING: Unsupported model for organization anonymization')
      return empty_db()

  def FindAddress(self, inputText, concat=False):
    if self.models['address']=='stanza':
      return self.Find_with_Stanza(inputText)
    elif self.models['address']=='spacy':
      return self.Find_with_Spacy(inputText, concat)
    elif self.models['address']=='':
      return empty_db()
    elif self.models['address']=='john':
      return self.Find_with_John(inputText, concat)
    else:
      print('WARNING: Unsupported model for address anonymization')
      return empty_db()

  def FindDate(self, inputText, concat=False):
    if self.date_level not in ['hide','year','month']:
      print('WARNING: Unsupported type of date anonymization. It should be hide, year or month.')
      return empty_db()
    if self.models['date']=='regex':
      matches = re.finditer('(?:\d{1,4}[-\\ \/ ])?(\d{1,2}|(?:gen(?:naio)?|feb(?:braio)?|mar(?:zo)?|apr(?:ile)?|mag(?:gio)|giu(?:gno)?|lug(?:lio)?|ago(?:sto)?|set(?:tembre)?|ott(?:obre)?|nov(?:embre)?|dic(?:embre)?))[-\\ \/  ]\d{1,4}',inputText.lower())
      span_list = [(match.span()[0], match.span()[1], match.group()) for match in matches]
      db = pd.DataFrame(span_list, columns=['start','end','text'])
      db['entity_type'] = 'DATA' #this is important to check for additional anonym. types for dates. if you change this, should also change the if in mask_data
      if concat: self.dbs = pd.concat((self.dbs, db))
      self.tracker.loc[self.tracker['entity_type']=='date','status']=True
      return db
    elif self.models['date']=='':
      return empty_db()
    elif self.models['date']=='john':
      return self.Find_with_John(inputText, concat)
    else:
      print('WARNING: Unsupported model for date anonymization')
      return empty_db()

  # ner functions for multiple entities, to avoid rerunning
  def Find_with_Stanza(self, inputText, concat=False):
    ents = self.tracker[self.tracker['model']=='stanza']
    ents_todo = ents[ents['status']==False]
    if len(ents_todo)>0: # check if there are still entities to find
      doc = self.m_stanza(inputText)
      span_list = [(e.start_char, e.end_char, e.type, e.text) for e in doc.ents]
      db = pd.DataFrame(span_list, columns=['start','end','entity_type','text'])
      db['entity_type'] = db['entity_type'].replace({'PER':'person','LOC':'address','ORG':'organization'})
      db = db[db['entity_type'].isin(ents_todo['entity_type'])].copy() # keep only entities selected with this model
      db['entity_type'] = db['entity_type'].replace({'person':'PERSONA','address':'INDIRIZZO','organization':'ORGANIZZAZIONE'})
      if concat: self.dbs = pd.concat((self.dbs, db))
      self.tracker.loc[self.tracker['entity_type'].isin(ents_todo['entity_type']),'status']=True # set detected entities as done
      return db
    else:
      return empty_db() # entities already found by another Find call of the same model

  def Find_with_Spacy(self, inputText, concat=False):
    ents = self.tracker[self.tracker['model']=='spacy']
    ents_todo = ents[ents['status']==False]
    if len(ents_todo)>0: # check if there are still entities to find
      doc = self.m_spacy(inputText)
      span_list = [(e.start_char, e.end_char, e.label_, e.text) for e in doc.ents]
      db = pd.DataFrame(span_list, columns=['start','end','entity_type','text'])
      db['entity_type'] = db['entity_type'].replace({'PER':'person','LOC':'address','ORG':'organization'})
      db = db[db['entity_type'].isin(ents_todo['entity_type'])].copy() # keep only entities selected with this model
      db['entity_type'] = db['entity_type'].replace({'person':'PERSONA','address':'INDIRIZZO','organization':'ORGANIZZAZIONE'})
      if concat: self.dbs = pd.concat((self.dbs, db))
      self.tracker.loc[self.tracker['entity_type'].isin(ents_todo['entity_type']),'status']=True # set detected entities as done
      return db
    else:
      return empty_db() # entities already found by another Find call of the same model

  def Find_with_John(self, inputText, concat=False):
    ents = self.tracker[self.tracker['model']=='john']
    ents_todo = ents[ents['status']==False]
    if len(ents_todo)>0: # check if there are still entities to find
      annotations = self.m_john.fullAnnotate(inputText)
      span_list = [(a.begin, a.end, a.metadata['entity'], a.result) for a in annotations[0]['ner_chunk'] if a.metadata['entity'] == 'SSN']
      db = pd.DataFrame(span_list, columns=['start','end','entity_type','text'])
      db['entity_type'] = db['entity_type'].replace({'DOCTOR':'person','PATIENT':'person', 'CITY':'address','HOSPITAL':'organization' ,'E-MAIL':'email', 'AGE':'age', 'CF':'fiscal_code', 'ZIP':'zipcode', 'TELEPHONE':'telephone'})
      db = db[db['entity_type'].isin(ents_todo['entity_type'])].copy() # keep only entities selected with this model
      db['entity_type'] = db['entity_type'].replace({'person':'PERSONA','address':'INDIRIZZO','organization':'ORGANIZZAZIONE', 'email':'E-MAIL', 'age':'ETA', 'fiscal_code':'CODICE_FISCALE', 'zipcode':'CAP', 'telephone':'TELEFONO'})
      if concat: self.dbs = pd.concat((self.dbs, db))
      self.tracker.loc[self.tracker['entity_type'].isin(ents_todo['entity_type']),'status']=True # set detected entities as done
      return db
    else:
      return empty_db() # entities already found by another Find call of the same model


if __name__ == '__main__':
  example = ''' In data 28/06/2022 abbiamo visitato il sig. Carlos Sieros di anni 66
  con zip code 50134 e anche 40120
  affetto da cardiomiopatia cronica all'ospedale Santa Maria delle Croci di Ravenna. Il signore lavora da Google.
  Il 10 9 2021 ha avuto un intervento chirurgico.  marina-61@virgilio.it.
  Il sign. Rossi ha come numero di telefono di casa 0574 569852.
  Si rimanda al prossimo controllo in data 4/09/2022. Gennaio 2020.
  Il paziente era accompagnato dalla figlia Viola Rossi con telefono +39 255 7401545.
  Da prendere al bisogno 72 mg di aspirina 7 gennaio 2020.
  Il 12/22 c'è stato il sole
  Il paziente lascia il suo numero di cellulare: 3841202587 valido fino al 18 MARZO 2021.
  Cordiali saluti,
  Dr. Fazeelat Abdullah.
  CF FZLBDL97E20E102W
  345/4722110

  18 gennaio 2021
  Via di Roma 25, Milano 48125
  7-1-2000
  '''
  deid = anonymizer('./config.json')
  output_dict = deid.deIdentificationIta(example)
  # print('output text:', output_dict['text'])
  # print('matches dataframe:', output_dict['match_dataframe'])
  # print(deid.mask_data(example, mode='tag_l',date_level='month',sc='-')) # example of just masking using different modes with already stored found entities
