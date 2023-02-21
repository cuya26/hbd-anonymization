
import json
import pandas as pd
import re

class CONLL2JSON:
    def __init__(self):
        self._sep_list = [' ', '\t']
        self._fmt_list = ['json', 'jsonl']

    @staticmethod
    def _load_text(txt_path):
        """
        Opens the container and reads file
        Returns a list[string]
        :param txt_path: filepath
        """
        with open(txt_path, 'rt', encoding="utf-8") as infile:
            content = infile.readlines()

        return content

    @staticmethod
    def _process_text(content, sep):
        """
        given a list of txt_paths
        -process each
        :param content: list of strings
        :param sep: string representing separator
        :return: list of dicts
        """
        list_dicts = []
        words = []
        
        # detect newline and make sentences
        for line_num, line in enumerate(content):
            index_sentence = 0 
            if line != '\n':
                words.append(line.strip('\n'))
            else:
                sentence = " ".join([word.split(sep)[0] for word in words])
                words = [word.split(sep) for word in words]

                df_words = pd.DataFrame(data=words, columns=['word', 'ner'])
                m = df_words['ner'].eq('O')

                try:

                    df_words_filtered = (df_words[~m].assign(Label=lambda x: x['ner'].str.replace('^[IB]-', ''))
                                         #.groupby([m.cumsum(), 'ner'])['word']
                                         #.agg(' '.join)
                                         #.droplevel(0)
                                         .reset_index())
                                         #.reindex(df_words.columns, axis=1))

                except ValueError:
                    words = []
                    continue
                label_w_pos = []
                
                for index, row in df_words_filtered.iterrows():
                    start_pos = sentence.find(row['word'],index_sentence)
                    end_post = start_pos + len(row['word']) + 1
                    index_sentence = end_post
                    label_w_pos.append([start_pos, end_post, row['ner']])

                list_dicts.append({'text': sentence, 'labels': label_w_pos})

                words = []
              
        return list_dicts

    @staticmethod
    def _write_text(list_dicts, fmt, output_file):
        """
        :param list_dicts: list of dicts formatted in json
        :param fmt: format of json file either JSON object or JSON Line file
        :param output_file: file to save data
        :return:
        """

    def parse(self, input_file: str, output_file: str, sep: str, fmt) -> None:
        if sep not in self._sep_list:
            raise RuntimeError(f'Separator should be in "{self._sep_list}", provided separator was "{sep}"')

        if fmt not in self._fmt_list:
            raise RuntimeError(f'Format should be in "{self._fmt_list}", provided file format was "{fmt}"')

        content = self._load_text(input_file)
        list_dicts = self._process_text(content, sep)

        if fmt == 'json':
            data = json.dumps(list_dicts, ensure_ascii=False)
        else:
            list_dicts = [json.dumps(l) for l in list_dicts]
            data = '\n'.join(list_dicts)

        with open(output_file, 'w',encoding="utf-8") as json_file:
            json_file.write(data)

"""# 1 -  Separazione da testo generale a singoli txt """

general_book = open('test_ita.txt').read()
general_book = general_book.replace('\n\n','\n')
general = general_book.split('COVID-19 O\n: O\ncaso O\n')
total_g = []
for e in general:
  if(len(e)>45):
    total_g.append('TEST_NLP test\n\n' + e + '\n\nTEST_NLP test')


for index,element in enumerate(total_g):
  with open(f'/content/ttmp_txt/txt_{index}.txt', 'w') as f:
    f.write(element)

"""# 2 - convertire con parser """



import os
x = os.listdir("/content/ttmp_txt/")
x.sort(key=lambda el: int(''.join(filter(str.isdigit, el))))
txtfiles = []

for index,file_name in enumerate(x):
    converter = CONLL2JSON()
    converter.parse(f'/content/ttmp_txt/{file_name}', f'/content/tjson_file/json_{index}.json', ' ', 'json')

"""# 3 - estrazione testo  da json"""


def HideEntity(text_input, ann_dataframe):
  #with open(text_input,'r') as ff:
  #  text_tmp = ff.read()
  #open text file in read mode
  text_file = open(text_input, "r")
  #read whole file to a string
  data = text_file.read()
  #close file
  text_file.close()
  anonymized_text_tmp= list(data)
  for index, row in ann_dataframe.iterrows():
       pos1 = row[0]
       pos2 = row[1]

       #if(len('<'+row['ann']+'>')>(pos2-pos1)):
       #  anonymized_text_tmp[pos1:pos2] = '*'*(pos2-pos1)
       #else:
       number_of_star = pos2-pos1-len('<'+row['ann']+'>') 
       anonymized_text_tmp[pos1:pos2] = '<'+row['ann']+'>'+'*'*number_of_star

  return  "".join(anonymized_text_tmp)

"""# 4 - Costruzione Annotazione con merge e creazione del testo con maschera"""

dir1 = os.listdir("/content/tjson_file")
dir1.sort(key=lambda el: int(''.join(filter(str.isdigit, el))))
column_to_mantain = ['I-DOCTOR','B-DOCTOR','B-ORGANIZATION','I-ORGANIZATION','B-HOSPITAL','I-HOSPITAL','B-CITY','B-COUNTRY','B-DATE','I -DATE','B-AGE']

for index_text,file_name in enumerate(dir1):
    f = open(f'/content/tjson_file/json_{index_text}.json', 'r')
    data = json.load(f)
    f.close()
    z= open(f'/content/ttxt/{index_text}.txt', 'w') 
    json_file = data.pop(1)
    z.write(json_file['text'])
    z.close()
    lista=json_file['labels']
    df = pd.DataFrame.from_records(lista)
    df_finale = df[df[2].isin(column_to_mantain)]
    df_finale["ann"] = " "
    df_finale.loc[df_finale[2] =='B-AGE','ann']= 'AGE'
    df_finale.loc[df_finale[2] =='B-DOCTOR','ann']= 'NAME'
    df_finale.loc[df_finale[2] =='B-ORGANIZATION','ann']= 'ORG'
    df_finale.loc[df_finale[2] =='B-HOSPITAL','ann']= 'ORG'
    df_finale.loc[df_finale[2] =='B-CITY','ann']= 'LOC'
    df_finale.loc[df_finale[2] =='B-COUNTRY','ann']= 'LOC'
    df_finale.loc[df_finale[2] =='B-DATE','ann']= 'DATE'
    for i, row in df_finale.iterrows():
      if(row['ann'] == ' '):
         df_finale.loc[(df_finale[1] == row[0]) & (df_finale[2].str.contains(row[2].split('-')[1])),1]=row[1]

    df2= df_finale.sort_values(0)
    df_finale = df2[df2['ann'] != ' ']
    #df_finale=df_finale.drop([2], axis=1) Manteniamo anche la colonna con entità originale
    a = open(f'/content/tanonimiz/{index_text}_anon.txt', 'w')
    result = HideEntity(f'/content/ttxt/{index_text}.txt',df_finale)
    a.write(result)
    a.close()
    df_finale.to_csv(f'/content/tann/{index_text}_anon.csv',index=False,header=False)

