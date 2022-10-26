import pandas as pd
from sklearn.tree import DecisionTreeClassifier

def optimal_binning_boundary(x: pd.Series, y: pd.Series, sample_weight: pd.Series, criterion='entropy',
                             max_leaf_nodes = 100, min_samples_leaf = 0.01, nan: float = -999.) -> list:
    '''
        Generate the best binning best on decision tree split nodes.
    '''
    boundary = []

    x = x.fillna(nan).values  # Filling null value
    y = y.values

    clf = DecisionTreeClassifier(criterion=criterion,
                                 max_leaf_nodes=max_leaf_nodes,
                                 min_samples_leaf=min_samples_leaf)

    clf.fit(x.reshape(-1, 1), y, sample_weight = sample_weight)  # Train the decision tree

    n_nodes = clf.tree_.node_count
    children_left = clf.tree_.children_left
    children_right = clf.tree_.children_right
    threshold = clf.tree_.threshold

    for i in range(n_nodes):
        if children_left[i] != children_right[i]:  # Get the threshold of leaf nodes
            boundary.append(threshold[i])

    boundary.sort()

    min_x = x.min() - 1
    max_x = x.max() + 1  # Make sure the max possible score can be binned.
    boundary = [min_x] + boundary + [max_x]

    return boundary