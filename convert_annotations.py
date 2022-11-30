def find_occurrences(s, ch):
    return [i for i, letter in enumerate(s) if letter == ch]


def build_csv_repr(text):
    out = ''
    starting_points = find_occurrences(text, '<')
    year_counter = 0
    for s in starting_points:
        t = ''
        i = s+1
        while text[i] != '>':
            t = t + text[i]
            i += 1
        i = i+1  # move over '>'
        while i < len(text) and text[i] == '#':
            i += 1
        if i < len(text) and text[i] != '#' and text[i] != '>':
            i = i-1
        s = s - 2 * year_counter
        if t == 'YEAR':
            year_counter += 1
        i = i - 2*year_counter
        out = out + str(s) + ';' + str(i) + ';' + t + '\n'
    return out


text = '''LETTERA DI DIMISSIONE 
 
Sig.ra <NAME>##### (nata il <DATE>####)
PSD001   
 
<LOC>##############, <LOC>#,  <DATE>####
Alla cortese attenzione 
del Medico Curante 
 
 
Alla cortese attenzione  
Etichetta paziente 
Ai Colleghi della  
Divisione di Cardiologia Riabilitativa 
<LOC>'''

if __name__ == '__main__':

    letter_name = '838768'
    with open('C:\\Users\\vitto\OneDrive - Politecnico di Milano\\HBD\\anonymisation_letters\\' + letter_name + '_anon.txt', 'r', encoding='utf-8') as fp:
        text = fp.read()

    csv_repr = build_csv_repr(text)
    with open('out.csv', 'w') as fp:
        fp.write(csv_repr)

    import pandas as pd
    df = pd.read_csv('out.csv', names=['begin', 'end', 'type'], sep=';')

    with open('C:\\Users\\vitto\OneDrive - Politecnico di Milano\\HBD\\anonymisation_letters\\' + letter_name + '.txt', 'r', encoding='utf-8') as fp:
        or_text = fp.read()
    for i, r in df.iterrows():
        print(r['type'], or_text[r['begin']:r['end']+1])
