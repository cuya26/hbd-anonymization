'''
De-identification code for Italian Clinical Text

'''

# import datefinder # to install with pip
# install Stanza with pip
import re
import dateutil.parser
# from typing import Match
from functools import reduce
import json


tc = '■' # temporary character for replacement


class anonymizer:
  def __init__(self, configfile):
    # parse configuration file
    with open(configfile) as cin:
      cfg = json.load(cin)
    
    self.models = cfg['models']
    self.mode = cfg['mask']['mode']
    self.sc = cfg['mask']['special_character'][0] # take only first character if a string is passed
    self.date_level = cfg['mask']['date_level']
    self.mask_modes = ['tag','tag_l','anon','anon_l']
    
    # initialize needed models
    print(f'{"DOWNLOADING AND INITIALIZING MODELS ":-<80}')
    for model in set(self.models.values()):
      if model=='stanza':
        import stanza # to install with pip
        stanza.download("it")
        self.m_stanza = stanza.Pipeline(lang="it", processors='tokenize, ner')
      elif model=='spacy':
       import spacy #to install with pip
       # Install 'python -m spacy download it_core_news_lg' 
       self.m_spacy = spacy.load("it_core_news_lg")
      elif model=='regex':
        # All the regex refers to Italian Style for telephone, zip code, etc ---
        pass # nothing to load
      elif model!='':
        print(f'{model} is not a supported model. Please check the documentation for the list of supported models for each field')
        
  # main function      
  def deIdentificationIta(self, inputText):
    #print(f'{"ANONYMIZING GIVEN TEXT ":-<80}')
    spans_dict = {}
    output_dict = self.HideTelephone(inputText)
    inputText = output_dict['text']
    spans_dict['telephone'] = output_dict['spans']
    #print('input text after telephone:', inputText)
    output_dict = self.HideZipCode(inputText)
    inputText = output_dict['text']
    spans_dict['zipcode'] = output_dict['spans']
    #print('input text after zipcode:', inputText)
    output_dict = self.HideEmail(inputText)
    inputText = output_dict['text']
    spans_dict['email'] = output_dict['spans']
    #print('input text after email:', inputText)
    output_dict = self.HidePerson(inputText)
    inputText = output_dict['text']
    spans_dict['person'] = output_dict['spans']
    #print('input text after person:', inputText)
    output_dict = self.HideOrganization(inputText)
    inputText = output_dict['text']
    spans_dict['organization'] = output_dict['spans']
    #print('input text after organization:', inputText)
    output_dict = self.HideAddress(inputText)
    inputText = output_dict['text']
    spans_dict['address'] = output_dict['spans']
    #print('input text after address:', inputText)
    output_dict = self.HideDate(inputText)
    inputText = output_dict['text']
    spans_dict['date'] = output_dict['spans']
    #print('input text after date:', inputText)
    output_dict = self.HideFiscalCode(inputText)
    inputText = output_dict['text']
    spans_dict['fiscal_code'] = output_dict['spans']
    #print('input text after fiscal code:', inputText)
    if self.mode=='anon':
      inputText = re.sub(f'{tc}+', tc*3, inputText) # you can set how long the fixed character replacement will be here
    if self.mode=='tag':
      inputText = inputText.replace(tc,'')
    return {'text': inputText.replace(tc,self.sc), 'spans_dict': spans_dict}
    
  # masker function. replace sensible text with tag name (optional) and special character
  def mask_data(self, inputText, ent_type, spans):
    if self.mode not in self.mask_modes:
      print(f'WARNING: Unsupported masking mode selected. Choose one from {self.mask_modes}.')
      return inputText
    if len(spans)==0:
      return inputText
    spans=[(0,0)]+spans 
    outText = ''
    for i in range(1,len(spans)):
      if self.mode=='anon_l' or self.mode=='anon':
        outText += inputText[spans[i-1][1]:spans[i][0]] + tc*(spans[i][1]-spans[i][0])
      elif self.mode=='tag_l' or self.mode=='tag':
        outText += inputText[spans[i-1][1]:spans[i][0]] + f'{ent_type:{tc}<{spans[i][1]-spans[i][0]}}'
        # !!! NB: se ent_type è più lungo del testo da rimpiazzare, il testo si allunga
    outText += inputText[spans[-1][1]:]
    return outText
    
  # anonymization functions
  def HideTelephone(self, inputText):
    if self.models['telephone']=='regex':
      matches = re.finditer('(?:((\+?\(?\d{2,3}\)?)|(\(\+?\d{2,3}\))) ?)?(((\d{2}[\ \-\.\/]?){3,5}\d{2})|((\d{3}[\ \-\.\/]?){2}\d{4}))', inputText)
      span_list = [match.span() for match in matches]
      outText = self.mask_data(inputText, '<TELEFONO>', span_list)
      return {'text': outText, 'spans': span_list}
    elif self.models['telephone']=='':
      return {'text': inputText, 'spans': None}
    else:
      print('WARNING: Unsupported model for telephone anonymization')
      return {'text': inputText, 'spans': None}
  
  def HideZipCode(self, inputText):
    if self.models['zipcode']=='regex':
      matches = re.finditer('\D([0-9]{5})\D', inputText)
      span_list = [match.span(1) for match in matches]
      outText = self.mask_data(inputText, '<CAP>', span_list)
      return {'text': outText, 'spans': span_list}
    elif self.models['zipcode']=='':
      return {'text': inputText, 'spans': None}
    else:
      print('WARNING: Unsupported model for zipcode anonymization')
      return {'text': inputText, 'spans': None}

  def HideEmail(self, inputText):
    if self.models['email']=='regex':
      matches = re.finditer('[^\s@]+@([^\s@.,]+\.)+[^\s@.,]{2,}', inputText)
      span_list = [match.span() for match in matches]
      outText = self.mask_data(inputText, '<E-MAIL>', span_list)
      return {'text': outText, 'spans': span_list}
    elif self.models['email']=='':
      return {'text': inputText, 'spans': None}
    else:
      print('WARNING: Unsupported model for e-mail anonymization')
      return {'text': inputText, 'spans': None}

  def HideFiscalCode(self, inputText):
    if self.models['fiscal_code']=='regex':
      # print(inputText)
      matches = re.finditer('[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]', inputText)
      span_list = [match.span() for match in matches]
      outText = self.mask_data(inputText, '<CF>', span_list)
      return {'text': outText, 'spans': span_list}
    elif self.models['fiscal_code']=='':
      return {'text': inputText, 'spans': None}
    else:
      print('WARNING: Unsupported model for fiscal code anonymization')
      return {'text': inputText, 'spans': None}

  def HidePerson(self, inputText):
    if self.models['person']=='stanza':
      doc = self.m_stanza(inputText)
      span_list = [(e.start_char, e.end_char) for e in doc.ents if e.type=='PER']
      outText = self.mask_data(inputText, '<PERSONA>', span_list)
      return {'text': outText, 'spans': span_list}
    elif self.models['person']=='spacy':
      doc = self.m_spacy(inputText)
      span_list = [(e.start_char, e.end_char) for e in doc.ents if e.label_=='PER']
      outText = self.mask_data(inputText, '<PERSONA>', span_list)
      return {'text': outText, 'spans': span_list}
    elif self.models['person']=='':
      return {'text': inputText, 'spans': None}
    else:
      print('WARNING: Unsupported model for person anonymization')
      return {'text': inputText, 'spans': None}
  
  def HideOrganization(self, inputText):
    if self.models['organization']=='stanza':
      doc = self.m_stanza(inputText)
      span_list = [(e.start_char, e.end_char) for e in doc.ents if e.type=='ORG']
      outText = self.mask_data(inputText, '<ORGANIZZAZIONE>', span_list)
      return {'text': outText, 'spans': span_list}
    elif self.models['organization']=='spacy':
      doc = self.m_spacy(inputText)
      span_list = [(e.start_char, e.end_char) for e in doc.ents if e.label_=='ORG']
      outText = self.mask_data(inputText, '<ORGANIZZAZIONE>', span_list)
      return {'text': outText, 'spans': span_list}
    elif self.models['organization']=='':
      return {'text': inputText, 'spans': None}
    else:
      print('WARNING: Unsupported model for organization anonymization')
      return {'text': inputText, 'spans': None}
  
  def HideAddress(self, inputText):
    if self.models['address']=='stanza':
      doc = self.m_stanza(inputText)
      span_list = [(e.start_char, e.end_char) for e in doc.ents if e.type=='LOC']
      outText = self.mask_data(inputText, '<INDIRIZZO>', span_list)
      return {'text': outText, 'spans': span_list}
    elif self.models['address']=='spacy':
      doc = self.m_spacy(inputText)
      span_list = [(e.start_char, e.end_char) for e in doc.ents if e.label_=='LOC']
      outText = self.mask_data(inputText, '<INDIRIZZO>', span_list)
      return {'text': outText, 'spans': span_list}
    elif self.models['address']=='':
      return {'text': inputText, 'spans': None}
    else:
      print('WARNING: Unsupported model for address anonymization')
      return {'text': inputText, 'spans': None}
  
  
  def HideDate(self, inputText):
    if self.models['date']=='regex':
      text_lower = inputText.lower()
      matches_dates = re.finditer('(?:\d{1,4}[-\\ \/ ])?(\d{1,2}|(?:gen(?:naio)?|feb(?:braio)?|mar(?:zo)?|apr(?:ile)?|mag(?:gio)|giu(?:gno)?|lug(?:lio)?|ago(?:sto)?|set(?:tembre)?|ott(?:obre)?|nov(?:embre)?|dic(?:embre)?))[-\\ \/  ]\d{1,4}',text_lower)
      tuple_months = ('gennaio','01'),('gen','01'),('febbraio','02'),('feb','02'),('marzo','03'),('mar','03'),('aprile','04'),('apr','04'),('maggio','05'),('mag','05'),('giugno','06'),('giu','06'),('luglio','07'),('lug','07'),('agosto','08'),('ago','08'),('settembre','09'),('set','09'),('ottobre','10'),('ott','10'),('novembre','11'),('nov','11'),('dicembre','12'),('dic','12')
      ent_type = '<DATA>'
      outText = ''
      last_span = 0
      span = None

      span_list = [match.span() for match in matches_dates]
      
      for match in matches_dates:
        data_to_hide = match.group()
        if(not(re.search('[a-zA-Z]', data_to_hide)) == None):
          # sostituisce il mese in stringa con l'equivalente numerico
          data_to_hide = reduce(lambda a, kv: a.replace(*kv), tuple_months, data_to_hide)
        try: 
          data_finder = dateutil.parser.parse(data_to_hide, dayfirst=True)
        except dateutil.parser.ParserError as e:
          print('WARNING: Date parsing error: ', e, '... Ignoring this match')
          continue
        span = match.span()
        outText += inputText[last_span:span[0]]
        last_span = span[1]
        
        if self.date_level == 'hide':
          if self.mode=='anon_l' or self.mode=='anon':
            outText += tc*(span[1]-span[0])
          elif self.mode=='tag_l' or self.mode=='tag':
            outText += f'{ent_type:{tc}<{span[1]-span[0]}}'
        elif self.date_level == 'year':
          outText += f'{data_finder.year}'
        elif self.date_level == 'month':
          outText += f'{data_finder.month}-{data_finder.year}'
        else:
          print('WARNING: Unsupported type of date anonymization')
          return {'text': inputText, 'spans': None}
      if span != None:
        outText += inputText[span[1]:]
        return {'text': outText, 'spans': span_list}
      else:
        return {'text': inputText, 'spans': []}
    
    elif self.models['date']=='':
      return {'text': inputText, 'spans': None}
    else:
      print('WARNING: Unsupported model for date anonymization')
      return {'text': inputText, 'spans': None}

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
  #print('output text:', output_dict['text'])
  #print('spans dict:', output_dict['spans_dict'])
  
