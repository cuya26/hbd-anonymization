# HBD-Data-Project
## Working Group 1: Text Analysis
### Anonymization
This repository contains scripts and projects on anonymization of clinical letters

## How to use
You can run the script by calling 
> python ita_deidentification.py
You can change the script behavior by editing the config.json file.  

## Evaluation
In order to evaluate a supported de-identification system:

1. Install docker in order to use a ready-to use system with all the requirements
2. Create a config file in the configs folder
3. Run the evaluation script with:
> docker run -it --entrypoint python3 -v $(pwd):/workspace 2603931630/hbd-demo-backend:latest evaluation_notebook.py new_config_file.json

Where new_config_file.json is the config file created in point 2 and located in the configs folder.

For windows user the $(pwd) have to be substitute with the path of the hbd-anonymization folder

The letters used for test are placed in the anonyization_letters folder

### Models
Set the desired models to use in the config.json file, under the "models" section. Leave the field empty if you don't want to anonymize it.  
Currently these are the fields that can be anonymized:
#### Telephone
Supported models: regex, john  
Covered cases (regex):  
>3491234567  
+393491234567  
00393491234567  
(+39)3491234567  
+(39) 349 8505734  
349 8505734  
349-8505734  
0543 721370  
0543721370  
06 43721370  
064 3721370  
064/3721370  
#### Zipcode
Supported models: regex, john  
Covered cases (regex):  
>47122  
01010  
#### Email
Supported models: regex, john  
Covered cases (regex):  
>mario.rossi@libero.it  
m.r@l.it  
m5.r2@v2.it  
#### Date
Supported models: regex, john  
Covered cases (regex):  
>28/06/2022  
10 9 2021  
4/09/2022  
Gennaio 2020.  
7 gennaio 2020  
18 gennaio 2021  
7-1-2000  
12/22  
#### Fiscal code
Supported models: regex, john  
Covered cases: Italian fiscal code for regex model, social security number for john model

#### Person
Supported models: stanza, spacy, john

#### Organization
Supported models: stanza, spacy, john

#### Address
Supported models: stanza, spacy, john

### Mask modes
Set the desired mask mode and special character in the "mask" section of the config.json file.  
Currently the following modes are supported:
* **tag** : The anonymized text is replaced with a tag containing the entity type (telephone,person etc.). _(default option)_
* **tag_l** : Same as tag but preserving original data length. Missing data is filled with the special character. [Note: Length is preserved only if the anonymized entity is longer than the tag.]
* **anon_l** : The anonymized text is gonna be converted to special character, preserving original data length.
* **anon** : Same as anon_l but anonymized text is replaced with fixed length special characters.
* **random (TODO!)** : Replace the anonymized text with a randomly generated text of the same entity type.

You can use any single character as **special_character** (default is star), which is gonna be used to replace the anonymized text.

With the "date_level" parameter you can choose how to anonymize dates. Supported modes are:
* **hide** : Anonymize dates according to the chosen mask mode _(default option)_
* **month** : Keep only the month
* **year** : Keep only the month and the year

### Models details
* **regex** : This model was developed by HBD-anonymization team to specifically recognize some italian entities (like fiscal code, telephone and postal code) and some more generic ones (like dates) using regular expressions. Supported entities are: **date**, **telephone**, **email**, **zipcode** and **fiscal code**.  
More info: https://en.wikipedia.org/wiki/Regular_expression
* **spacy** : spaCy is an open-source software library for advanced natural language processing. The model used in this script is it_core_news_lg, a pipeline comprehending tokenization, lemmatization and named entity recognition, trained on a large news database. Supported entities are: **person**, **organization** and **address**.  
More info: https://spacy.io/models/it#it_core_news_lg
* **stanza** : stanza is The Stanford NLP Group's official Python NLP library. The model used in this script is the italian pipeline, comprehending tokenization and NER, trained on FBK dataset. Supported entities are: **person**, **organization** and **address**.  
More info: https://stanfordnlp.github.io/stanza/ner_models.html
* **john** : JohnSnowLabs is an american AI & NLP company that helps healthcare & life science organizations. The model used in this script is clinical_deidentification italian, part of the Healthcare NLP licensed package, comprehending tokenization and NER. Supported entities are: **person**, **organization**, **address**, **date**, **telephone**, **email**, **zipcode**, **fiscal code** and **age**.  
More info: https://nlp.johnsnowlabs.com/2022/03/28/clinical_deidentification_it_2_4.html

