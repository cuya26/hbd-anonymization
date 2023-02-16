#!/usr/bin/env python
# coding: utf-8

import sys
import pandas as pd
import json
from ita_deidentification import anonymizer
from os import path, listdir
from collections import defaultdict

import seaborn as sns
import matplotlib.pyplot as plt

sns.set_theme()

def compute_scores(predictions, targets, confusion_only=False):
    tp = 0
    fp = 0
    fn = 0
    targets = targets.copy()
    for prediction in predictions:
        if prediction in targets:
            tp += 1
            targets.remove(prediction)
        else:
            fp += 1
    fn += len(targets)
    
    precision = tp / (tp + fp) if tp + fp > 0 else 0
    recall = tp / (tp + fn) if tp + fn > 0 else 0
    f1 = 2 * (precision * recall) / (precision + recall) if precision + recall > 0 else 0
    if confusion_only:
        return { 'tp': tp, 'fp': fp, 'fn': fn }
    else:
        return { 'precision': precision, 'recall': recall, 'f1': f1 }


def compute_scores_document(df_predictions, df_targets):
    # strict calculation
    df_predictions['strict_pos'] = df_predictions.start.astype(str) + '-' + df_predictions.end.astype(str)
    df_targets['strict_pos'] = df_targets.start.astype(str) + '-' + df_targets.end.astype(str)

    strict_scores_dict = {}
    for entity_type in df_targets.entity_type.unique():
        strict_scores_dict[entity_type] = compute_scores(df_predictions.loc[df_predictions.entity_type == entity_type,'strict_pos'].to_list(), df_targets.loc[df_targets.entity_type == entity_type, 'strict_pos'].to_list())

    # lenient calculation
    def overlap(a, b):
        if max(0, min(a[1], b[1]) - max(a[0], b[0])) != 0:
            return True
        else:
            return False

    lenient_scores_dict = {}
    for entity_type in df_targets.entity_type.unique():
        # print('compute score for entity_type:', entity_type)
        prediction_lenient_match = []
        for pred_index, prediction in df_predictions.loc[df_predictions.entity_type == entity_type].iterrows():
            for target_index, target in df_targets.loc[df_targets.entity_type == entity_type].iterrows():
                prediction_pos = [prediction['start'], prediction['end']]
                target_pos = [target['start'], target['end']]
                if overlap(prediction_pos, target_pos):
                    prediction_lenient_match.append(target_index)
        
        prediction_lenient_match = list(set(prediction_lenient_match))

        prediction_lenient_match = prediction_lenient_match + [None] * (len(df_predictions.loc[df_predictions.entity_type == entity_type]) - len(prediction_lenient_match))
        # print('prediction_lenient_match:', prediction_lenient_match)
        # print('target indexes', df_targets.index[df_targets.entity_type == entity_type].tolist())
        lenient_scores_dict[entity_type] = compute_scores(
            prediction_lenient_match,
            df_targets.index[df_targets.entity_type == entity_type].tolist()
        )

    return {'strict': strict_scores_dict, 'lenient': lenient_scores_dict}


if __name__ == "__main__":

    test_folder_path = './anonymisation_letters/'
    config_path = './configs/' + sys.argv[1]
    with open(config_path, 'r') as json_file:
        config_dict = json.load(json_file)
    config_name = config_dict['name']
    print('config name:', config_name)
    deid = anonymizer(config_path)

    test_filename_text_list = [filename for filename in listdir(test_folder_path) if '.txt' in filename and '_' not in filename]
    test_filename_text_list.sort()
    test_filename_anon_list = [filename for filename in listdir(test_folder_path) if 'anon.csv' in filename]
    test_filename_anon_list.sort()


    index_named_entity = [
        'TEL',
        'ZIP',
        'EMAIL',
        'NAME',
        'ORG',
        'LOC',
        'DATE',
        'CF',
        'AGE'
    ]
    corpus_scores_lenient_dict = {key: dict(precision=[], recall=[], f1=[]) for key in index_named_entity}
    corpus_scores_strict_dict = {key: dict(precision=[], recall=[], f1=[]) for key in index_named_entity}
    for filename_anon, filename_text in zip(test_filename_anon_list, test_filename_text_list):
        # load text
        with open(test_folder_path + filename_text, 'r') as file:
            text = ''.join(file.readlines())
        # perform inference and retrieve spans
        output = deid.deIdentificationIta(text)
        df_predictions = output['match_dataframe']

        df_predictions['entity_type'] = df_predictions['entity_type'].replace(
            [
                'TELEFONO',
                'CAP',
                'E-MAIL',
                'PERSONA',
                'ORGANIZZAZIONE',
                'INDIRIZZO',
                'DATA',
                'CF',
                'ETÀ'
            ],
            [
                'TEL',
                'ZIP',
                'EMAIL',
                'NAME',
                'ORG',
                'LOC',
                'DATE',
                'CF',
                'AGE'
            ]
        )
        
        # load the gold standard annotations
        df_targets = pd.read_csv(test_folder_path + filename_anon, sep=';', names=['start', 'end', 'entity_type'])
        df_targets.end = df_targets.end + 1
        df_targets['text'] = df_targets.apply(lambda row: text[row['start']:row['end']], axis=1)
        df_targets['entity_type'] = df_targets['entity_type'].replace(
            [
                'YEAR'
            ],
            [
                'DATE'
            ]
        )

        # compute the scores of the single document
        scores_dict = compute_scores_document(df_predictions, df_targets)
        
        # insert the lenient scores in a aggregated list
        for entity_type in scores_dict['lenient']:
            for score_type in ['precision','recall', 'f1']:
                corpus_scores_lenient_dict[entity_type][score_type].append(scores_dict['lenient'][entity_type][score_type])
        # insert the strict scores in a aggregated list
        for entity_type in scores_dict['strict']:
            for score_type in ['precision','recall', 'f1']:
                corpus_scores_strict_dict[entity_type][score_type].append(scores_dict['strict'][entity_type][score_type])

    # create the dataframe of the scores of the corpus
    df_corpus_scores_lenient = pd.DataFrame(index=index_named_entity, columns=['precision', 'recall', 'f1'])
    df_corpus_scores_strict = pd.DataFrame(index=index_named_entity, columns=['precision', 'recall', 'f1'])

    # compute the avg of the scores: correspond to a macro average 
    for entity_type in index_named_entity:
        for score_type in ['precision','recall', 'f1']:

            score_list_lenient = corpus_scores_lenient_dict[entity_type][score_type]
            if score_list_lenient != []:
                df_corpus_scores_lenient.loc[entity_type, score_type] = sum(score_list_lenient)/len(score_list_lenient)

            score_list_strict = corpus_scores_strict_dict[entity_type][score_type]
            if score_list_strict != []:
                df_corpus_scores_strict.loc[entity_type, score_type] = sum(score_list_strict)/len(score_list_strict)

    # drop entity types that never appears 
    df_corpus_scores_lenient.dropna(inplace=True)
    df_corpus_scores_strict.dropna(inplace=True)

    df_corpus_scores_lenient.recall = df_corpus_scores_lenient.recall.astype(float)
    df_corpus_scores_lenient.precision = df_corpus_scores_lenient.precision.astype(float)
    df_corpus_scores_lenient.f1 = df_corpus_scores_lenient.f1.astype(float)

    df_corpus_scores_strict.recall = df_corpus_scores_strict.recall.astype(float)
    df_corpus_scores_strict.precision = df_corpus_scores_strict.precision.astype(float)
    df_corpus_scores_strict.f1 = df_corpus_scores_strict.f1.astype(float)

    # plot the heatmap of the lenient scores
    sns.set(font_scale=2)
    fig, ax = plt.subplots(figsize=(10,10))
    sns.heatmap(df_corpus_scores_lenient, cmap='RdYlGn', annot=True, fmt=".2f", annot_kws={"fontsize":20}, vmin=0, vmax=1)
    fig.savefig(f'./evaluations/{config_name}_lenient_heat_scores.png', dpi=400, bbox_inches='tight')


    # plot the heatmap of the strict scores
    sns.set(font_scale=2)
    fig, ax = plt.subplots(figsize=(10,10))
    sns.heatmap(df_corpus_scores_strict, cmap='RdYlGn', annot=True, fmt=".2f", annot_kws={"fontsize":20}, vmin=0, vmax=1)
    fig.savefig(f'./evaluations/{config_name}_strict_heat_scores.png', dpi=400, bbox_inches='tight')

    plt.clf()


    df_corpus_scores_lenient['score_type'] = 'lenient'
    df_corpus_scores_strict['score_type'] = 'strict'

    df_corpus_scores_lenient = df_corpus_scores_lenient.rename_axis('entity').reset_index()
    df_corpus_scores_strict= df_corpus_scores_strict.rename_axis('entity').reset_index()

    df_corpus_scores = pd.concat([df_corpus_scores_strict, df_corpus_scores_lenient], ignore_index=True)
    df_corpus_scores.to_csv(f'./evaluations/{config_name}_scores.csv')

    evaluation_folder_path = './evaluations/'
    filenames_scores = [filename for filename in listdir(evaluation_folder_path) if '_scores.csv' in filename]
    filenames_scores.sort()

    df_scores = pd.DataFrame()

    for filename_scores in filenames_scores:
        config_name = '_'.join(filename_scores.split('_')[:-1])
        df_scores_filename = pd.read_csv(evaluation_folder_path + filename_scores)
        df_scores_filename['config_name'] = config_name
        df_scores = pd.concat([df_scores, df_scores_filename], ignore_index=True)

    plt.figure(figsize=(30,15))

    for metric in ['precision', 'recall', 'f1']:

        comparison_lenient_plot = sns.barplot(x="entity",
                y=metric,
                hue="config_name",
                data=df_scores[df_scores['score_type']=='lenient'].sort_values(['entity', metric])
            )
        # comparison_lenient_plot.legend(loc='upper right')
        comparison_lenient_plot.get_figure().savefig(f'./evaluations/lenient_comparison_{metric}.png', dpi=400, bbox_inches='tight')

        plt.clf()

        comparison_strict_plot = sns.barplot(x="entity",
                y=metric,
                hue="config_name",
                data=df_scores[df_scores['score_type']=='strict'].sort_values(['entity', metric])
        )
        comparison_strict_plot.get_figure().savefig(f'./evaluations/strict_comparison_{metric}.png', dpi=400, bbox_inches='tight')

        plt.clf()