from tensorflow.keras import losses
import tensorflow as tf

class AELoss(losses.Loss):

    def __init__(self, cat_dict):
        super(AELoss, self).__init__()
        self.ce = losses.CategoricalCrossentropy()
        self.cat_dict=cat_dict

    def call(self, y_true, y_pred):
        tot_ce, pos = 0.0, 0
        for i, (k, v) in enumerate(self.cat_dict.items()):
            tot_ce += self.ce(y_true[:, pos:pos + v], y_pred[:, pos:pos + v]+1e-8 )
            pos += v

        norm_cats = len(self.cat_dict)
        cat_loss = tot_ce / norm_cats
        return cat_loss*100

    @classmethod
    def from_config(cls, config):
        return cls(**config)
