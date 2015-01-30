Assignment 7 Group 6
====================

How to prepare:

1. Check-out repository
2. Import as maven repository in eclipse
3. RefereeSentiment class contains the Kafka spout config, change if necessary
4. When finished, run `mvn clean package; storm jar target/group6assignment7-0.1.jar nl.utwente.bigdata.topology.RefereeSentiment RefereeSentiment local


Used libraries
--------------
- twitter4j: http://twitter4j.org/en/index.html
- AFINN: AFINN is a list of English words rated for valence with an integer
between minus five (negative) and plus five (positive).
- storm-hdfs

References
----------
- Baccianella, S., Esuli, A., & Sebastiani, F. (2010). SentiWordNet 3.0: An Enhanced Lexical Resource for Sentiment Analysis and Opinion Mining. In N. C. (Conference Chair), K. Choukri, B. Maegaard, J. Mariani, J. Odijk, S. Piperidis, … D. Tapias (Eds.), Proceedings of the Seventh International Conference on Language Resources and Evaluation (LREC’10). Valletta, Malta: European Language Resources Association (ELRA).

- Nielsen, F. Å. (2011). AFINN. Informatics and Mathematical Modelling, Technical University of Denmark.

Pig Latin MapReduce WordNet
----------------------------
<!-- Yeah not really ruby but this colors it quite okay'ish -->
```ruby
wn_eng = LOAD 'wordnet/wn-wikt-eng.tab' USING PigStorage('\t') AS (synid_eng: chararray, type_eng:chararray, word_eng:chararray);
wn_dut = LOAD 'wordnet/wn-wikt-nld.tab' USING PigStorage('\t') AS (synid_dut: chararray, type_dut:chararray, word_dut:chararray);
wn_ger = LOAD 'wordnet/wn-wikt-deu.tab' USING PigStorage('\t') AS (synid_ger: chararray, type_ger:chararray, word_ger:chararray);
wn_fra = LOAD 'wordnet/wn-wikt-fra.tab' USING PigStorage('\t') AS (synid_fra: chararray, type_fra:chararray, word_fra:chararray);
wn_fra = LOAD 'wordnet/wn-wikt-ita.tab' USING PigStorage('\t') AS (synid_ita: chararray, type_ita:chararray, word_ita:chararray);
wn_spa = LOAD 'wordnet/wn-wikt-spa.tab' USING PigStorage('\t') AS (synid_spa: chararray, type_spa:chararray, word_spa:chararray);
afinn = LOAD 'wordnet/afinn-111.txt' USING PigStorage('\t') AS (word:chararray,sentscore:int);

wn_join = JOIN wn_eng BY synid_eng,
    wn_dut BY synid_dut,
    wn_ger BY synid_ger,
    wn_fra BY synid_fra,
    wn_spa BY synid_spa,
    wn_ita BY synid_ita
;

en_combi = FOREACH wn_join GENERATE synid_eng, LOWER(word_eng) as word_eng;
en_grouped = JOIN en_combi BY word_eng, afinn BY word;
en_clean = FOREACH en_grouped GENERATE word_eng, sentscore;
en_cleand = DISTINCT en_clean;

nl_combi = FOREACH wn_join GENERATE synid_eng, LOWER(word_eng) as word_eng, LOWER(word_dut) as word_dut;
nl_grouped = JOIN nl_combi BY word_eng, afinn BY word;
nl_clean = FOREACH nl_grouped GENERATE word_dut, sentscore;
nl_cleand = DISTINCT nl_clean;


de_combi = FOREACH wn_join GENERATE synid_eng, LOWER(word_eng) as word_eng, LOWER(word_ger) as word_ger;
de_grouped = JOIN de_combi BY word_eng, afinn BY word;
de_clean = FOREACH de_grouped GENERATE word_ger, sentscore;
de_cleand = DISTINCT de_clean;

fr_combi = FOREACH wn_join GENERATE synid_eng, LOWER(word_eng) as word_eng, LOWER(word_fra) as word_fra;
fr_grouped = JOIN fr_combi BY word_eng, afinn BY word;
fr_clean = FOREACH fr_grouped GENERATE word_fra, sentscore;
fr_cleand = DISTINCT fr_clean;

it_combi = FOREACH wn_join GENERATE synid_eng, LOWER(word_eng) as word_eng, LOWER(word_ita) as word_ita;
it_grouped = JOIN it_combi BY word_eng, afinn BY word;
it_clean = FOREACH it_grouped GENERATE word_ita, sentscore;
it_cleand = DISTINCT it_clean;

es_combi = FOREACH wn_join GENERATE synid_eng, LOWER(word_eng) as word_eng, LOWER(word_spa) as word_spa;
es_grouped = JOIN es_combi BY word_eng, afinn BY word;
es_clean = FOREACH es_grouped GENERATE word_spa, sentscore;
es_cleand = DISTINCT es_clean;

STORE en_cleand INTO 'senti-en.txt';
STORE nl_cleand INTO 'senti-nl.txt';
STORE de_cleand INTO 'senti-de.txt';
STORE fr_cleand INTO 'senti-fr.txt';
STORE it_cleand INTO 'senti-it.txt';
STORE ES_cleand INTO 'senti-es.txt';
```
