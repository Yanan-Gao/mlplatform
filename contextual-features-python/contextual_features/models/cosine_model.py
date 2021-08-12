from sentence_transformers import SentenceTransformer, util
from pyspark.sql.functions import col, concat_ws, substring
from pyspark.sql.functions import pandas_udf
from typing import Iterator, Tuple
import pandas as pd
import torch
from urllib import parse
import gc
import re
import numpy as np
from contextlib import nullcontext

class CosineModel:
    def __init__(self, model_weights_path, iab_ids, iab_names, url_threshold, device=0, default_class="UNK"):
        """Initialises a Cosine Similarity Waterfall Model

        Args:
            model_weights_path (String): Path to model weights on local file system or to download from huggingface (local recommended)
            iab_ids ([Integer]): List of iab category identifiers
            iab_names ([String]): List of text strings associated with iab_ids for running cosine similarity against
            url_threshold ([type]): Threshold for waterfall model not to look at the text content
        """
        # load weights into cpu to avoid CUDA serialisation error in pandas_udf
        self.model = SentenceTransformer(model_weights_path, device='cpu')
        self.device = device
        self.default_class = default_class
        self.iab_names = np.array(iab_names)
        self._un, self.iab_ids = self.unique_id_names(self.iab_names, iab_ids)
        self.url_threshold = url_threshold
        self.label_embeddings = self.model.encode([c for c in self.iab_names if c != ''], convert_to_tensor=False, show_progress_bar=False)

        self.set_up_regex()

    def set_up_regex(self):
        regs = [
           "https://",
           "www.",
           "fandom.com",
           ".com",
           ".co.uk",
           ".html",
           ".php",
           "/?[0-9]+"
        ]
        self.reg = re.compile("(" + "|".join(regs) + ")")
        self.regGG = re.compile("(.gg/)")

    @staticmethod
    def unique_id_names(iab_names, iab_ids):
        """Removes duplicate categories from given iab names and ids.

        Args:
            iab_names ([string]): IAB names
            iab_ids ([int]): IAB ids

        Returns:
            ([string], [int]): De-duplicated IAB names and ids.
        """
        unique_names, zero_indexed_ids, seen, i = [], [], set(), -1
        for index, name in zip(iab_ids, iab_names):
            if not(index in seen):
                seen.add(index)
                unique_names.append(name)
                i += 1
            zero_indexed_ids.append(i)
        return np.array(unique_names, dtype=str), np.array(zero_indexed_ids, dtype=int)
        
    def clean_url(self, url):
        """Heuristics for cleaning a url before classification

        Args:
            url (string): The url

        Returns:
            string: Cleaned Url string
        """
        # remove trailing digits
        return re.sub(self.reg, "", re.sub(self.regGG, " game/", url))

    def predict_argmax(self, embedding, k):
        """
        predicts top-k argmax between input embedding es and IAB taxonomy
        returns indexes into each iab_name and the raw cosine similarity values
        """
        css = np.array(util.pytorch_cos_sim(embedding, self.label_embeddings))
        cosine_scores = np.argsort(-css, axis=1).reshape(-1).astype(int)
        actual_indexes = self.iab_ids[cosine_scores]
        indexes, scores, seen = [], [], set()
        for index, score in zip(actual_indexes, css[0, cosine_scores]):
            if not(index in seen):
                seen.add(index)
                indexes.append(index)
                scores.append(score)
            if len(scores) >= k:
                break
        return indexes, scores

    def get_context(self):
        if self.device != 'cpu':
            # call model with automatic mixed precision
            return torch.cuda.amp.autocast()
        else:
            return nullcontext()
    
    def get_pandas_udf(self, top_k):
            """ Returns cosine similarity transformation udf that can be serialised """
            @pandas_udf("id:array<int>, class:array<string>, score:array<float>, finalized:array<boolean>")
            def predict_batch_udf(batches: Iterator[Tuple[pd.Series, pd.Series]]) -> Iterator[pd.DataFrame]:
                # Expensive initialisation outside of batch loop
                self.model.eval()
                self.model.to(self.device)
                iab_names = np.insert(self._un, len(self._un), self.default_class)
                with self.get_context():
                    for url_signal, text_signal in batches:
                        preds = self.predict([(url_signal, self.url_threshold), (text_signal, 0.0)], top_k)
                        yield pd.DataFrame([(p[0].astype(int), iab_names[p[0].astype(int)], p[1], CosineModel.binary_heuristics(p[1])) for p in preds], columns=["id", "class", "score", "finalized"])
            return predict_batch_udf
    
    def classify_text(self, texts, top_k, clean_url=False):
        """Encodes and predicts classes from texts.
        Further de-duplicates indices from matching iab_names with the same iab_id.
        If we abstain from prediction the score will be -1.

        Args:
            texts ([String]): Texts to classify
            top_k (int): Top k classes to retain

        Returns:
            [[[float]]]: 3d array where input text corresponds to [top_k_prediction_ids, top_k_prediction_scores]
        """
        texts = list(map(self.clean_url, texts)) if clean_url else texts
        mask = list(map(self.filter_text, texts))
        texts = np.array(texts)[mask]

        es = self.model.encode(texts, convert_to_tensor=False, show_progress_bar=False, batch_size=64, device=self.device)
        preds = (self.predict_argmax(e, top_k) for e in es)
        default = [len(self._un)]*top_k, [-1]*top_k
        return np.array([(next(preds) if m else default) for m in mask], dtype=np.float32)

    def predict(self, signal_and_thresholds, top_k):
        """ Waterfall model on input signals based on thresholds """
        first_signal, threshold = signal_and_thresholds[0]
        preds = self.classify_text(first_signal.array, top_k, clean_url=True)
        for next_signal, next_threshold in signal_and_thresholds[1:]:
            mask = preds[:,1,0] < threshold # if top score below threshold we will look at the next signal
            filtered_batch = next_signal.array[mask]
            if len(filtered_batch) > 0:
                next_preds = self.classify_text(filtered_batch, top_k)
                preds[mask] = [self.merge_preds([p1, p2], top_k) for p1, p2 in zip(preds[mask], next_preds)]
            threshold = next_threshold
        return preds

    @staticmethod
    def filter_text(text):
        return (len(text) >= 5)

    @staticmethod
    def merge_preds(prediction_list, top_k):
        """ Merge top k predictions by max operator """
        known = dict()
        for pred_indexes_and_scores in prediction_list:
            for ind, score in zip(pred_indexes_and_scores[0], pred_indexes_and_scores[1]):
                # merge scores at each index by max operator
                known[ind] = max(score, known.get(ind, -1.0))
        svals = list(known.items())
        svals.sort(key=lambda x: -x[1])
        # repeat last element so that we don't end up with a ragged array
        while len(svals) < top_k:
            svals.append(svals[-1])
        return list(zip(*svals[:top_k]))

    def transform(self, df, title_col, text_col, top_k):
        """ Transforms df with cosine similarity model """
        model_udf = self.get_pandas_udf(top_k)
        return df\
            .withColumn("preds", model_udf(col(title_col), col(text_col)))

    @staticmethod
    def binary_heuristics(sorted_scores):
        """ Takes sorted ids and scores and returns ids that meet the following heuristics and thresholds """
        threshold_from_top = 0.06
        constant_threshold = 0.25
        categories = []
        top_threshold = sorted_scores[0] - threshold_from_top
        topk = 4
        for score in sorted_scores[:topk]:
            categories.append(bool((score > constant_threshold) and (score > top_threshold)))
        return categories
