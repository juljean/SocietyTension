import re

#VOWEL = re.compile('[аеиоуюяіїє]')
INFINITIVE = re.compile(r'(ти|учи|ячи|вши|ши|ати|яти|ючи)$')
PERFECTIVEGROUND = re.compile(r'((ив|ивши|ившись))$')
# static $PERFECTIVEGROUND = '/((ив|ивши|ившись|ыв|ывши|ывшись((?<=[ая])(в|вши|вшись)))$/u';
# http://uk.wikipedia.org/wiki/Рефлексивне_дієслово
REFLEXIVE = re.compile(r'(с[яьи])$')
#http://uk.wikipedia.org/wiki/Прикметник + http://wapedia.mobi/uk/Прикметник
ADJECTIVE = re.compile(r'(ими|ій|ий|а|е|ова|ове|ів|є|їй|єє|еє|я|ім|ем|им|ім|их|іх|ою|йми|іми|у|ю|ого|ому|ої)$')
#http://uk.wikipedia.org/wiki/Дієприкметник
PARTICIPLE = re.compile(r'(ий|ого|ому|им|ім|а|ій|у|ою|ій|і|их|йми|их)$')
#http://uk.wikipedia.org/wiki/Дієслово
VERB = re.compile(r'(сь|ся|ив|ать|ять|у|ю|ав|али|учи|ячи|вши|ши|е|ме|ати|яти|є)$')
#http://uk.wikipedia.org/wiki/Іменник
NOUN = re.compile(r'(а|ев|ов|е|ями|ами|еи|и|ей|ой|ий|й|иям|ям|ием|ем|ам|ом|о|у|ах|иях|ях|ы|ь|ию|ью|ю|ия|ья|я|і|ові|ї|ею|єю|ою|є|еві|ем|єм|ів|їв|\'ю)$')
RVRE = re.compile(r'^(.*?[аеиоуюяіїє])(.*)$')
DERIVATIONAL = re.compile(r'[^аеиоуюяіїє][аеиоуюяіїє]+[^аеиоуюяіїє]+[аеиоуюяіїє].*сть?$')

def stemWord(word: str) -> str:
    stem = word.lower()

    # check if infinitive
    m = INFINITIVE.sub('', word)
    if m != word:
        return word

    # init
    p = RVRE.search(stem)
    if not p:
        return stem
    start = p.group(1)
    RV = p.group(2)
    if not start or not RV:
        return stem

    # STEP 1
    m = PERFECTIVEGROUND.sub('', RV)
    if m == RV:
        RV = REFLEXIVE.sub('', RV)
        m = ADJECTIVE.sub('', RV)
        if m == RV:
            RV = PARTICIPLE.sub('', RV)
        else:
            RV = m
            m = VERB.sub('', RV)
            if m == RV:
                RV = NOUN.sub('', RV)
            else:
                RV = m
    else:
        RV = m

    # STEP 2
    if RV.endswith('і'):
        RV = RV[:-1]

    # STEP 3
    if DERIVATIONAL.search(RV):
        RV = re.sub(r'ість?$', '', RV)

    # STEP 4
    m = re.sub(r'ь?$', '', RV);
    if m == RV:
        RV = re.sub(r'ейше?', '', RV)
        RV = re.sub(r'нн$', 'н', RV)
    else:
        RV = m

    stem = start + RV

    return stem


def stemArray(tokens: list):
    return list(map(stemWord, tokens))
