import data_analysis as da
import visualization as viz


def main():
    print("=" * 60)
    print("АНАЛИЗ ДАННЫХ О СОСТОЯНИИ ПЛОДА")
    print("=" * 60)
    print()
    
    df = da.load_data('fetal_health.csv')
    print(f"Данные загружены: {len(df)} записей, {len(df.columns)} признаков")
    print()
    
    print("-" * 60)
    stats = da.get_basic_statistics(df)
    print(f"Общее количество наблюдений: {stats['total_samples']}")
    print(f"Количество признаков: {stats['total_features']}")
    print(f"Пропущенные значения: {stats['missing_values']}")
    print()
    
    print("Распределение классов:")
    class_labels = da.get_class_labels()
    for class_val, count in sorted(stats['class_distribution'].items()):
        label = class_labels.get(class_val, f"Класс {class_val}")
        percentage = stats['class_percentages'].get(class_val, 0)
        print(f"  {label}: {count} ({percentage:.2f}%)")
    print()
    
 
    print("Кореляция")
    print("-" * 60)
    top_features = da.get_top_correlated_features(df, top_n=5)
    print("Признаки с наибольшей корреляцией с состоянием плода:")
    for feature, corr in top_features.items():
        print(f"  {feature}: {corr:.4f}")
    print()
    

    viz.plot_class_distribution(df)
    viz.plot_correlation_heatmap(df)
    viz.plot_top_correlations(df, top_n=10)


    print("Статистика по важным признакам")
    print("-" * 60)
    for feature in top_features.head(3).index:
        print(f"\nПризнак: {feature}")
        feature_stats = da.get_feature_statistics(df, feature)
        print(f"  Среднее: {feature_stats['mean']:.4f}")
        print(f"  Медиана: {feature_stats['median']:.4f}")
        print(f"  Стандартное отклонение: {feature_stats['std']:.4f}")
        print(f"  Минимум: {feature_stats['min']:.4f}")
        print(f"  Максимум: {feature_stats['max']:.4f}")

    viz.keep_figures_open()


if __name__ == "__main__":
    main()
