'''
De-identification code for Italian Clinical Text

'''

import stanza # to install with pip
import datefinder # to install with pip
import re
import dateutil.parser
from typing import Match
from functools import reduce

'''
Possible variable to hide

--- All the regex refers to Italian Style for telephone, zip code, etc ---
'''
TELEPHONE = 1
ZIPCODE = 2
EMAIL = 3
PERSON = 4
ORGANIZATION = 5
ADDRESS = 6
DATE = 7
CF = 8

stanza.download("it")
nlp_it = stanza.Pipeline(lang="it", processors='tokenize, ner')

''' Choice from user '''
toHide=[1,2,3,4,5,6,7,8]
levelOfAnonymization = 0 # 0 -> Hide Date; 1 -> Keep only the year

'''
Switch action from user choice
- The '*' replace is need by mantain the entity position for Stanza and date matcher
- Substitution of the entity is necessary for better understanding
- The order of ifs is important to maintain consistency with entity substitutions, based on the elements in the list all those to be substituted are checked
'''
def deIdentificationIta(inputText,toHide, levelOfAnonymization):
  if TELEPHONE in toHide:
    inputText = HideTelephone(inputText)
  if ZIPCODE in toHide:
    inputText = HideZipCode(inputText)
  if EMAIL in toHide:
    inputText = HideEmail(inputText)
  if PERSON in toHide:
    inputText = HidePerson(inputText)
  if ORGANIZATION in toHide:
    inputText = HideOrganization(inputText)
  if ADDRESS in toHide:
    inputText = HideAddress(inputText)
  if DATE in toHide:
    inputText = HideDate(inputText, levelOfAnonymization)
  if CF in toHide:
    inputText = HideCF(inputText)
  return inputText.replace('*','').replace('<PER>','<PERSONA>').replace('<LOC>','<INDIRIZZO>').replace('<ORG>','<ORGANIZZAZIONE>')

''' 
Covered case:
3491234567
+393491234567
00393491234567
(+39)3491234567
+(39) 349 8505734
349 8505734
3498505734 
+39 349 850 5734
349 850 5734
0543 721370
0543721370
06 43721370
064 3721370
'''
def HideTelephone(inputText): 
  matchesPhone = re.finditer('(?:((\+?\(?\d{2,3}\)?)|(\(\+?\d{2,3}\))) ?)?(((\d{2}[\ \-\.]?){3,5}\d{2})|((\d{3}[\ \-\.]?){2}\d{4}))',inputText)
  anonymized_text_tmp = list(inputText)
  for match in matchesPhone:
    phone_to_hide = match.group()
    index = inputText.find(match.group())
    number_of_star = len(match.group())-len('<TELEFONO>')
    anonymized_text_tmp[index:index+len(match.group())] = '<TELEFONO>'+'*'*number_of_star

  return "".join(anonymized_text_tmp)

'''
Necessary step to identify telephone numbers to avoid ambiguites with number in zip code

Covered case:
47122
01010

'''
def HideZipCode(inputText):
  anonymized_text_tmp = list(inputText)
  if TELEPHONE not in toHide:
    inputText = HideTelephone(inputText)

  matchesCap = re.finditer('[0-9]{5}',inputText)
  
  for match in matchesCap:
    cap_to_hide = match.group()
    index = inputText.find(match.group())
    number_of_star = len(match.group())-len('<CAP>')
    anonymized_text_tmp[index:index+len(match.group())] = '<CAP>'+'*'*number_of_star

  return "".join(anonymized_text_tmp)

'''
Covered case:
mario.rossi@libero.it
m.r@l.it
m5.r2@v2.it

'''
def HideEmail(inputText):
  return re.sub('[^\s@]+@([^\s@.,]+\.)+[^\s@.,]{2,}','<E-MAIL>',inputText)

def HideCF(inputText):
  return re.sub('[A-Z]{6}\d{2}[A-Z]\d{2}[A-Z]\d{3}[A-Z]','<CF>',inputText)


def HidePerson(inputText):
  return HideWithStanza(inputText,'PER')

def HideOrganization(inputText):
  return HideWithStanza(inputText,'ORG')

def HideAddress(inputText):
  return HideWithStanza(inputText,'LOC')

'''
Replace entity with * not to lose the position
''' 
def HideWithStanza(inputText, entity_type):
  doc = nlp_it(inputText)
  anonymized_text_tmp = list(inputText)
  for entity in doc.ents:
    if entity_type == entity.type:
       pos1 = entity.start_char
       pos2 = entity.end_char
       number_of_star = pos2-pos1-len('<'+entity.type+'>')
       anonymized_text_tmp[pos1:pos2] = '<'+entity.type+'>'+'*'*number_of_star
  return  "".join(anonymized_text_tmp)

'''
Necessary step to identify telephone numbers to avoid ambiguites with number in dates

Covered case:
28/06/2022
10 9 2021 
4/09/2022
Gennaio 2020.
7 gennaio 2020
18 gennaio 2021
7-1-2000
12/22
'''

def HideDate(inputText, levelOfAnonymization):
  text_lower = inputText.lower()
  if TELEPHONE not in toHide:
    text_lower = HideTelephone(text_lower)
  matches_dates = re.finditer('(?:\d{1,4}[-\\ \/ ])?(\d{1,2}|(?:gen(?:naio)?|feb(?:braio)?|mar(?:zo)?|apr(?:ile)?|mag(?:gio)|giu(?:gno)?|lug(?:lio)?|ago(?:sto)?|set(?:tembre)?|ott(?:obre)?|nov(?:embre)?|dic(?:embre)?))[-\\ \/  ]\d{1,4}',text_lower)
  anonymized_text_tmp = list(inputText)

  tuple_months = ('gennaio','01'),('gen','01'),('febbraio','02'),('feb','02'),('marzo','03'),('mar','03'),('aprile','04'),('apr','04'),('maggio','05'),('mag','05'),('giugno','06'),('giu','06'),('luglio','07'),('lug','07'),('agosto','08'),('ago','08'),('settembre','09'),('set','09'),('ottobre','10'),('ott','10'),('novembre','11'),('nov','11'),('dicembre','12'),('dic','12')

  for match in matches_dates:
    data_to_hide = match.group()
    if(not(re.search('[a-zA-Z]', data_to_hide)) == None):
      # sostituisce il mese in stringa con l'equivalente numerico
      data_to_hide = reduce(lambda a, kv: a.replace(*kv), tuple_months, data_to_hide)
      
      
    data_finder = dateutil.parser.parse(data_to_hide)
    index = text_lower.find(match.group())
    if levelOfAnonymization == 0:
      number_of_star = len(match.group())-len('<DATA>')
      anonymized_text_tmp[index:index+len(match.group())] = '<DATA>'+'*'*number_of_star
    else:
      number_of_star = len(match.group())-len(str(data_finder.year))
      anonymized_text_tmp[index:index+len(match.group())] = str(data_finder.year)+'*'*number_of_star

  return "".join(anonymized_text_tmp)

'''
Dummie example


TELEPHONE = 1
ZIPCODE = 2
EMAIL = 3
PERSON = 4
ORGANIZATION = 5
ADDRESS = 6
DATE = 7
CF = 8
'''

example = ''' In data 28/06/2022 abbiamo visitato il sig. Carlos Sieros di anni 66
affetto da cardiomiopatia cronica all'ospedale Santa Maria delle Croci di Ravenna. Il signore lavora da Google.
Il 10 9 2021 ha avuto un intervento chirurgico.  marina-61@virgilio.it.
Il sign. Rossi ha come numero di telefono di casa 0574 569852.
Si rimanda al prossimo controllo in data 4/09/2022. Gennaio 2020.
Il paziente era accompagnato dalla figlia Viola Rossi con telefono +39 255 7401545.
Da prendere al bisogno 72 mg di aspirina 7 gennaio 2020. 
mail del paziente alice.andalo@gmail.com e martina.cavallucci@gmail.com. 
Il 12/22 c'è stato il sole
Il paziente lascia il suo numero di cellulare: 3841202587 valido fino al 18 MARZO 2021.
Cordiali saluti,
Dr. Fazeelat Abdullah. 
CF FZLBDL97E20E102W

18 gennaio 2021
Via di Roma 25, Milano 48125
7-1-2000
'''

print(deIdentificationIta(example,toHide,1))
