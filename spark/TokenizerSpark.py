# from tokenization import SimpleTokenizer, SimpleLemmatizer



class CharNGramTokenizerSpark:
    def __init__(self, char_ngram = 3):
        self.char_ngram = char_ngram

    def tokenize(self, text, vocab = None):
        text = text.replace(" ", "").lower()
        tokens = [text[i:i+self.char_ngram] for i in range(0, len(text) - self.char_ngram)]
        if vocab is not None:
            vocab = vocab.value
            tokens = [tkn for tkn in tokens if tkn in vocab]

        return tokens

    def transform_docs(self, docs, vocab=None):

        def _spark_transform_doc(docid, title, body, vocab):
            title_tokens = self.tokenize(title, vocab)
            body_tokens = self.tokenize(body, vocab)

            return (docid, title_tokens, body_tokens)

        res = docs.map(lambda p: _spark_transform_doc(p[0], p[1], p[2], vocab))

        return res

    def transform_queries(self, queries):

        def _spark_transform_query(queryid, body):

            body_tokens = self.tokenize(body)

            return (queryid, body_tokens)

        res = queries.map(lambda p: _spark_transform_query(p[0], p[1]))

        return res

#
# class TokenizerSpark:
#     def __init__(self, tokenizer_class, lemmatizer_class):
#         self.tokenizer_class = tokenizer_class
#         self.lemmatizer_class = lemmatizer_class
#         pass
#
#     def transform_docs(self, docs, vocab = None):
#
#         def _spark_transform_doc(p):
#             docid, title, body =  p[0], p[1], p[2]
#             tkn = self.tokenizer_class()
#             lemm = self.lemmatizer_class()
#             title_tokens = tkn.fit_transform(title)
#             body_tokens = tkn.fit_transform(body)
#
#             title_lemms = lemm.fit_transform(title_tokens)
#             body_lemms = lemm.fit_transform(body_tokens)
#
#             return (docid, title_lemms, body_lemms)
#
#
#         res = docs.map(lambda p: _spark_transform_doc(p))
#
#         return res
#
#     def transform_queries(self, queries):
#
#         def _spark_transform_query(queryid, body):
#             tkn = self.tokenizer_class()
#             lemm = self.lemmatizer_class()
#             body_tokens = tkn.fit_transform(body)
#
#             body_lemms = lemm.fit_transform(body_tokens)
#
#             return (queryid, body_lemms)
#
#         res = queries.map(lambda p: _spark_transform_query(p[0], p[1]))
#
#         return res

