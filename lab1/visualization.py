import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np


try:
    plt.style.use('seaborn-v0_8-darkgrid')
except OSError:
    try:
        plt.style.use('seaborn-darkgrid')
    except OSError:
        plt.style.use('default')
sns.set_palette("husl")


plt.rcParams['toolbar'] = 'None'
plt.ion() 
import time


def keep_figures_open():
    try:
        while plt.get_fignums():
            plt.pause(0.1)
            time.sleep(0.1)
    except KeyboardInterrupt:
        plt.close('all')


def _hide_toolbar(fig):
    try:
        if hasattr(fig.canvas, 'toolbar') and fig.canvas.toolbar is not None:
            fig.canvas.toolbar.pack_forget()
    except (AttributeError, TypeError):
        pass
    try:
        if hasattr(fig.canvas.manager, 'toolbar') and fig.canvas.manager.toolbar is not None:
            fig.canvas.manager.toolbar.hide()
    except (AttributeError, TypeError):
        pass


def plot_class_distribution(df, save_path=None):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(8, 3.5))
    
    class_counts = df['fetal_health'].value_counts().sort_index()
    class_labels = {1.0: 'Normal', 2.0: 'Suspect', 3.0: 'Pathological'}
    labels = [class_labels.get(x, x) for x in class_counts.index]
    
    ax1.bar(labels, class_counts.values, color=['#2ecc71', '#f39c12', '#e74c3c'])
    ax1.set_title('Распределение классов состояния плода', fontsize=10, fontweight='bold')
    ax1.set_xlabel('Класс состояния плода', fontsize=9)
    ax1.set_ylabel('Количество наблюдений', fontsize=9)
    ax1.grid(axis='y', alpha=0.3)
    ax1.tick_params(labelsize=8)
    
    colors = ['#2ecc71', '#f39c12', '#e74c3c']
    ax2.pie(class_counts.values, labels=labels, autopct='%1.1f%%', 
            colors=colors, startangle=90, textprops={'fontsize': 8})
    ax2.set_title('Процентное распределение классов', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')

    plt.show(block=False)

    plt.pause(0.2)
    _hide_toolbar(fig)

    plt.pause(0.1)


def plot_correlation_heatmap(df, save_path=None):
    fig = plt.figure(figsize=(8, 6))
    correlation_matrix = df.corr()
    
    mask = np.triu(np.ones_like(correlation_matrix, dtype=bool))
    sns.heatmap(correlation_matrix, mask=mask, annot=False, cmap='coolwarm', 
                center=0, square=True, linewidths=0.3, cbar_kws={"shrink": 0.6},
                xticklabels=False, yticklabels=False)
    
    plt.title('Корреляционная матрица признаков', fontsize=11, fontweight='bold', pad=10)
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')

    plt.show(block=False)

    plt.pause(0.2)
    _hide_toolbar(fig)

    plt.pause(0.1)


def plot_top_correlations(df, top_n=10, save_path=None):
    correlations = df.corr()['fetal_health'].drop('fetal_health').abs().sort_values(ascending=False).head(top_n)
    
    fig = plt.figure(figsize=(6, 3.5))
    colors = ['#e74c3c' if x < 0 else '#2ecc71' for x in df.corr()['fetal_health'].drop('fetal_health').loc[correlations.index]]
    
    bars = plt.barh(range(len(correlations)), correlations.values, color=colors)
    plt.yticks(range(len(correlations)), correlations.index, fontsize=8)
    plt.xlabel('Абсолютная корреляция с fetal_health', fontsize=9)
    plt.title(f'Признаки с наибольшей корреляцией с состоянием плода', 
              fontsize=10, fontweight='bold')
    plt.grid(axis='x', alpha=0.3)
    plt.tick_params(axis='x', labelsize=8)
    
    for i, (idx, val) in enumerate(correlations.items()):
        plt.text(val + 0.01, i, f'{val:.3f}', va='center', fontsize=7)
    
    plt.tight_layout()
    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')

    plt.show(block=False)
    
    plt.pause(0.2)
    _hide_toolbar(fig)

    plt.pause(1)
