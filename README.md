Assignment 7 Group 6
====================

How to prepare:

1. Check-out repository
2. Import as maven repository in eclipse
3. RefereeSentiment class contains the Kafka spout config, change if necessary
4. When finished, run `mvn clean package; storm jar target/group6assignment7-0.1.jar nl.utwente.bigdata.RefereeSentiment RefereeSentiment local



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
WN_ENG = LOAD 'wordnet/wn-wikt-eng.tab' USING PigStorage('\t') AS (synid_eng: chararray, type_eng:chararray, word_eng:chararray);
WN_DUT = LOAD 'wordnet/wn-wikt-nld.tab' USING PigStorage('\t') AS (synid_dut: chararray, type_dut:chararray, word_dut:chararray);
WN_GER = LOAD 'wordnet/wn-wikt-deu.tab' USING PigStorage('\t') AS (synid_ger: chararray, type_ger:chararray, word_ger:chararray);
WN_FRA = LOAD 'wordnet/wn-wikt-fra.tab' USING PigStorage('\t') AS (synid_fra: chararray, type_fra:chararray, word_fra:chararray);
WN_ITA = LOAD 'wordnet/wn-wikt-ita.tab' USING PigStorage('\t') AS (synid_ita: chararray, type_ita:chararray, word_ita:chararray);
WN_SPA = LOAD 'wordnet/wn-wikt-spa.tab' USING PigStorage('\t') AS (synid_spa: chararray, type_spa:chararray, word_spa:chararray);
/*WN_RUS = LOAD 'wordnet/wn-wikt-rus.tab' USING PigStorage('\t') AS (synid_rus: chararray, type_rus:chararray, word_rus:chararray);*/
AFINN = LOAD 'wordnet/AFINN-111.txt' USING PigStorage('\t') AS (word:chararray,sentscore:int);


WN_JOIN = JOIN WN_ENG BY synid_eng,
WN_DUT BY synid_dut,
WN_GER BY synid_ger,
WN_FRA BY synid_fra,
WN_SPA BY synid_spa,
WN_ITA BY synid_ita
;

EN_COMBI = FOREACH WN_JOIN GENERATE synid_eng, LOWER(word_eng) as word_eng;
EN_GROUPED = JOIN EN_COMBI BY word_eng, AFINN BY word;
EN_CLEAN = FOREACH EN_GROUPED GENERATE word_eng, sentscore;
EN_CLEAND = DISTINCT EN_CLEAN;

NL_COMBI = FOREACH WN_JOIN GENERATE synid_eng, LOWER(word_eng) as word_eng, LOWER(word_dut) as word_dut;
NL_GROUPED = JOIN NL_COMBI BY word_eng, AFINN BY word;
NL_CLEAN = FOREACH NL_GROUPED GENERATE word_dut, sentscore;
NL_CLEAND = DISTINCT NL_CLEAN;


DE_COMBI = FOREACH WN_JOIN GENERATE synid_eng, LOWER(word_eng) as word_eng, LOWER(word_ger) as word_ger;
DE_GROUPED = JOIN DE_COMBI BY word_eng, AFINN BY word;
DE_CLEAN = FOREACH DE_GROUPED GENERATE word_ger, sentscore;
DE_CLEAND = DISTINCT DE_CLEAN;

FR_COMBI = FOREACH WN_JOIN GENERATE synid_eng, LOWER(word_eng) as word_eng, LOWER(word_fra) as word_fra;
FR_GROUPED = JOIN FR_COMBI BY word_eng, AFINN BY word;
FR_CLEAN = FOREACH FR_GROUPED GENERATE word_fra, sentscore;
FR_CLEAND = DISTINCT FR_CLEAN;

IT_COMBI = FOREACH WN_JOIN GENERATE synid_eng, LOWER(word_eng) as word_eng, LOWER(word_ita) as word_ita;
IT_GROUPED = JOIN IT_COMBI BY word_eng, AFINN BY word;
IT_CLEAN = FOREACH IT_GROUPED GENERATE word_ita, sentscore;
IT_CLEAND = DISTINCT IT_CLEAN;

ES_COMBI = FOREACH WN_JOIN GENERATE synid_eng, LOWER(word_eng) as word_eng, LOWER(word_spa) as word_spa;
ES_GROUPED = JOIN ES_COMBI BY word_eng, AFINN BY word;
ES_CLEAN = FOREACH ES_GROUPED GENERATE word_spa, sentscore;
ES_CLEAND = DISTINCT ES_CLEAN;

STORE EN_CLEAND INTO 'senti-en.txt';
STORE NL_CLEAND INTO 'senti-nl.txt';
STORE DE_CLEAND INTO 'senti-de.txt';
STORE FR_CLEAND INTO 'senti-fr.txt';
STORE IT_CLEAND INTO 'senti-it.txt';
STORE ES_CLEAND INTO 'senti-es.txt';
```
