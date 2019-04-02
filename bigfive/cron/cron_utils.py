import sys
sys.path.append('portrait/user')
from portrait.user.sentiment_classification.triple_sentiment_classifier import triple_classifier
from portrait.user.user_ip import from_ip_get_info

from textrank4zh import TextRank4Keyword, TextRank4Sentence

# key_words
def text_rank_keywords(text, keywords_num=5):
    keywords = []
    tr4w = TextRank4Keyword()
    tr4w.analyze(text=text.encode('utf-8', 'ignore'), lower=True, window=2)   # py2中text必须是utf8编码的str或者unicode对象，py3中必须是utf8编码的bytes或者str对象
    for item in tr4w.get_keywords(keywords_num, word_min_len= 1):
        keywords.append(item.word)

    return keywords

if __name__ == "__main__":
    # text_rank_keywords("今天是个好日子，心想的事儿都能成！", 5)
    # tweet = {'text': '还是要+点肉//@爱心china桃:没肉不好吃～'}
    # domain = triple_classifier(tweet)
    # print(domain)
    print(from_ip_get_info('172.16.181.112'))