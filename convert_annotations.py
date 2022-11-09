def find_occurrences(s, ch):
    return [i for i, letter in enumerate(s) if letter == ch]


def build_csv_repr(text):
    out = ''
    starting_points = find_occurrences(text, '<')
    for s in starting_points:
        t = ''
        i = s+1
        while text[i] != '>':
            t = t + text[i]
            i += 1
        i = i+1  # consumes '>'
        while i < len(text) and text[i] == '#':
            i += 1
        if i<len(text) and text[i] != '#' and text[i] != '>':
            i = i-1
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

    csv_repr = build_csv_repr(text)
    with open('out.csv', 'w') as fp:
        fp.write(csv_repr)
