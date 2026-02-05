import pandas as pd
import numpy as np


def load_data(file_path='fetal_health.csv'):
    df = pd.read_csv(file_path)
    return df


def get_basic_statistics(df):
    stats = {
        'total_samples': len(df),
        'total_features': len(df.columns) - 1,
        'missing_values': df.isnull().sum().sum(),
        'class_distribution': df['fetal_health'].value_counts().to_dict(),
        'class_percentages': (df['fetal_health'].value_counts(normalize=True) * 100).to_dict()
    }
    return stats


def get_feature_statistics(df, feature_name):

    feature_stats = {
        'mean': df[feature_name].mean(),
        'median': df[feature_name].median(),
        'std': df[feature_name].std(),
        'min': df[feature_name].min(),
        'max': df[feature_name].max(),
        'q25': df[feature_name].quantile(0.25),
        'q75': df[feature_name].quantile(0.75)
    }
    return feature_stats


def get_correlation_with_target(df, target_column='fetal_health'):
    correlations = df.corr()[target_column].drop(target_column).sort_values(ascending=False)
    return correlations


def get_top_correlated_features(df, target_column='fetal_health', top_n=10):
    correlations = get_correlation_with_target(df, target_column)
    return correlations.head(top_n)


def prepare_data_for_analysis(df):
    X = df.drop('fetal_health', axis=1)
    y = df['fetal_health']
    return X, y


def get_class_labels():
    return {
        1.0: 'Normal (Нормальное)',
        2.0: 'Suspect (Подозрительное)',
        3.0: 'Pathological (Патологическое)'
    }
