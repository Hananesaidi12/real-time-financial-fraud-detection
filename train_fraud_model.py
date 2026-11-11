from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, hour, dayofweek, avg, window
from pyspark.sql.window import Window
import os
import time
from dotenv import load_dotenv
load_dotenv()

# -------------------------------
# 1️⃣ SparkSession
# -------------------------------
spark = SparkSession.builder \
    .appName("FraudDetectionML_Enhanced") \
    .master("local[*]") \
    .config("spark.jars", "postgresql-42.7.8.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# -------------------------------
# 2️⃣ Charger données depuis PostgreSQL
# -------------------------------
df = spark.read.format("jdbc").options(
    url="jdbc:postgresql://localhost:5432/financial_dw",
    dbtable="transactions_clean",
    user="postgres",
    password=os.getenv("DB_PASSWORD"),
    driver="org.postgresql.Driver"
).load()

print("✅ Nombre de transactions totales :", df.count())
print("📊 Répartition des classes :")
df.groupBy("is_fraud_simulated").count().show()

# -------------------------------
# 3️⃣ Convertir is_fraud_simulated en entier (0/1)
# -------------------------------
df = df.withColumn("is_fraud_simulated", col("is_fraud_simulated").cast("integer"))

# -------------------------------
# 4️⃣ Feature Engineering - Ajout de features supplémentaires
# -------------------------------
print("🔧 Création de nouvelles features...")

# Extraire l'heure et le jour de la semaine du timestamp
df = df.withColumn("hour_of_day", hour(col("timestamp"))) \
       .withColumn("day_of_week", dayofweek(col("timestamp")))

# Calculer le montant moyen par utilisateur
window_spec = Window.partitionBy("user_id").orderBy(col("timestamp")).rowsBetween(-50, -1)
df = df.withColumn("user_avg_amount", avg("amount").over(window_spec))

# Calculer le ratio montant/moyenne utilisateur
df = df.withColumn("amount_to_avg_ratio", 
                   col("amount") / col("user_avg_amount"))

# Remplacer les valeurs NaN par 1 (si pas de moyenne disponible)
df = df.fillna({"amount_to_avg_ratio": 1.0, "user_avg_amount": 0.0})

# Ajouter une feature booléenne pour les pays à haut risque
high_risk_countries = ["RU", "NG", "IR", "KP"]
df = df.withColumn("is_high_risk_country", 
                   col("country").isin(high_risk_countries).cast("integer"))

print("✅ Features créées : hour_of_day, day_of_week, amount_to_avg_ratio, is_high_risk_country")

# -------------------------------
# 5️⃣ Encodage des variables catégorielles
# -------------------------------
index_type = StringIndexer(
    inputCol="transaction_type",
    outputCol="transaction_type_index",
    handleInvalid="keep"
)

index_country = StringIndexer(
    inputCol="country",
    outputCol="country_index",
    handleInvalid="keep"
)

# -------------------------------
# 6️⃣ Assembler les features
# -------------------------------
assembler = VectorAssembler(
    inputCols=[
        "amount", 
        "transaction_type_index", 
        "country_index",
        "hour_of_day",
        "day_of_week",
        "amount_to_avg_ratio",
        "is_high_risk_country"
    ],
    outputCol="features",
    handleInvalid="skip"
)

# -------------------------------
# 7️⃣ Modèle Random Forest avec équilibrage des classes
# -------------------------------
# Calculer les poids des classes
fraud_count = df.filter(col("is_fraud_simulated") == 1).count()
normal_count = df.filter(col("is_fraud_simulated") == 0).count()
total_count = df.count()

fraud_weight = total_count / (2.0 * fraud_count) if fraud_count > 0 else 1.0
normal_weight = total_count / (2.0 * normal_count) if normal_count > 0 else 1.0

print(f"\n⚖️  Équilibrage des classes:")
print(f"   - Transactions normales: {normal_count} (poids: {normal_weight:.2f})")
print(f"   - Transactions frauduleuses: {fraud_count} (poids: {fraud_weight:.2f})")

# Ajouter une colonne de poids
df = df.withColumn("class_weight", 
                   col("is_fraud_simulated") * fraud_weight + 
                   (1 - col("is_fraud_simulated")) * normal_weight)

rf = RandomForestClassifier(
    labelCol="is_fraud_simulated",
    featuresCol="features",
    weightCol="class_weight",  # Utiliser les poids des classes
    numTrees=100,
    maxDepth=7,
    maxBins=32,
    seed=42,
    featureSubsetStrategy="sqrt",
    impurity="gini"
)

# -------------------------------
# 8️⃣ Pipeline ML
# -------------------------------
pipeline = Pipeline(stages=[
    index_type,
    index_country,
    assembler,
    rf
])

# -------------------------------
# 9️⃣ Split train / test avec séparation temporelle
# -------------------------------
# Trier par timestamp pour une séparation temporelle réaliste
df = df.orderBy("timestamp")

# Calculer l'indice de split pour 80/20
split_idx = int(df.count() * 0.8)
train_df = df.limit(split_idx)
test_df = df.subtract(train_df)

print(f"\n🎓 Données d'entraînement (premiers 80% temporels): {train_df.count()}")
print(f"🧪 Données de test (derniers 20% temporels): {test_df.count()}")

# Vérifier la répartition des classes dans chaque dataset
print("\n📊 Distribution dans le training set:")
train_df.groupBy("is_fraud_simulated").count().show()

print("📊 Distribution dans le test set:")
test_df.groupBy("is_fraud_simulated").count().show()

# -------------------------------
# 🔟 Validation croisée pour optimiser les hyperparamètres
# -------------------------------
print("\n🎯 Configuration de la validation croisée...")

paramGrid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [50, 100, 150]) \
    .addGrid(rf.maxDepth, [3, 5, 7]) \
    .addGrid(rf.maxBins, [16, 32, 64]) \
    .build()

evaluator_cv = BinaryClassificationEvaluator(
    labelCol="is_fraud_simulated",
    metricName="areaUnderROC"
)

crossval = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator_cv,
    numFolds=3,  # Validation croisée à 3 folds
    seed=42,
    parallelism=2  # Paralléliser pour accélérer
)

print("🚀 Lancement de la validation croisée (peut prendre quelques minutes)...")
start_time = time.time()
cvModel = crossval.fit(train_df)
end_time = time.time()

print(f"✅ Validation croisée terminée en {end_time - start_time:.2f} secondes")

# Meilleur modèle
best_model = cvModel.bestModel
print(f"\n🏆 Meilleurs hyperparamètres trouvés:")
print(f"   - numTrees: {best_model.stages[-1].getNumTrees}")
print(f"   - maxDepth: {best_model.stages[-1].getMaxDepth()}")
print(f"   - maxBins: {best_model.stages[-1].getMaxBins()}")

# -------------------------------
# 1️⃣1️⃣ Évaluation sur le test set
# -------------------------------
print("\n🧪 Évaluation sur le test set...")
predictions = best_model.transform(test_df)

# Afficher quelques prédictions
print("\n📋 Exemples de prédictions :")
predictions.select(
    "transaction_id",
    "amount",
    "country",
    "is_fraud_simulated",
    "prediction",
    "probability"
).show(10, truncate=False)

# Calculer l'AUC-ROC
evaluator_auc = BinaryClassificationEvaluator(
    labelCol="is_fraud_simulated",
    rawPredictionCol="rawPrediction",
    metricName="areaUnderROC"
)

auc = evaluator_auc.evaluate(predictions)
print(f"\n📊 AUC-ROC du modèle : {auc:.4f}")

# Calculer la précision
accuracy_evaluator = MulticlassClassificationEvaluator(
    labelCol="is_fraud_simulated",
    predictionCol="prediction",
    metricName="accuracy"
)

accuracy = accuracy_evaluator.evaluate(predictions)
print(f"🎯 Précision du modèle : {accuracy:.4f}")

# Calculer le F1-Score
f1_evaluator = MulticlassClassificationEvaluator(
    labelCol="is_fraud_simulated",
    predictionCol="prediction",
    metricName="f1"
)

f1 = f1_evaluator.evaluate(predictions)
print(f"⚡ F1-Score du modèle : {f1:.4f}")

# Calculer la précision et le rappel pour la classe "fraude" (1)
from pyspark.sql.functions import expr

# Matrice de confusion
print("\n📈 Matrice de confusion :")
predictions.groupBy("is_fraud_simulated", "prediction").count().orderBy("is_fraud_simulated", "prediction").show()

# Calculer précision et rappel manuellement
TP = predictions.filter((col("is_fraud_simulated") == 1) & (col("prediction") == 1)).count()
FP = predictions.filter((col("is_fraud_simulated") == 0) & (col("prediction") == 1)).count()
FN = predictions.filter((col("is_fraud_simulated") == 1) & (col("prediction") == 0)).count()

precision_fraud = TP / (TP + FP) if (TP + FP) > 0 else 0
recall_fraud = TP / (TP + FN) if (TP + FN) > 0 else 0

print(f"\n🎯 Métriques pour la détection de fraude (classe 1):")
print(f"   - Précision: {precision_fraud:.4f}")
print(f"   - Rappel: {recall_fraud:.4f}")
print(f"   - F1-Score: {2 * precision_fraud * recall_fraud / (precision_fraud + recall_fraud) if (precision_fraud + recall_fraud) > 0 else 0:.4f}")

# Afficher l'importance des features
print("\n🔍 Importance des features :")
feature_importances = best_model.stages[-1].featureImportances
feature_names = [
    "amount", 
    "transaction_type", 
    "country", 
    "hour_of_day",
    "day_of_week", 
    "amount_to_avg_ratio",
    "is_high_risk_country"
]

for i, (feature, importance) in enumerate(zip(feature_names, feature_importances)):
    print(f"   {i+1}. {feature}: {importance:.4f}")

# -------------------------------
# 1️⃣2️⃣ Sauvegarde du modèle amélioré
# -------------------------------
best_model.write().overwrite().save("rf_fraud_model_enhanced")
print("\n💾 Modèle amélioré sauvegardé dans : rf_fraud_model_enhanced/")

# -------------------------------
# 1️⃣3️⃣ Test sur quelques exemples synthétiques
# -------------------------------
print("\n🧪 Test sur des exemples synthétiques :")

# Créer quelques exemples de test
test_examples = spark.createDataFrame([
    ("TX-TEST-001", 50.0, "online", "FR", 0, "2024-01-01 14:30:00", "U001"),
    ("TX-TEST-002", 1500.0, "online", "RU", 1, "2024-01-01 03:30:00", "U002"),
    ("TX-TEST-003", 3000.0, "atm", "NG", 1, "2024-01-01 22:15:00", "U003"),
    ("TX-TEST-004", 200.0, "pos", "DE", 0, "2024-01-01 12:00:00", "U004"),
], ["transaction_id", "amount", "transaction_type", "country", "is_fraud_simulated", "timestamp", "user_id"])

# Appliquer les mêmes transformations
test_examples = test_examples.withColumn("timestamp", col("timestamp").cast("timestamp"))
test_examples = test_examples.withColumn("hour_of_day", hour(col("timestamp")))
test_examples = test_examples.withColumn("day_of_week", dayofweek(col("timestamp")))
test_examples = test_examples.withColumn("is_high_risk_country", 
                                        col("country").isin(high_risk_countries).cast("integer"))

# Pour les exemples de test, utiliser une moyenne utilisateur par défaut
test_examples = test_examples.withColumn("user_avg_amount", lit(500.0))
test_examples = test_examples.withColumn("amount_to_avg_ratio", col("amount") / col("user_avg_amount"))

# Faire les prédictions
test_predictions = best_model.transform(test_examples)

print("📋 Prédictions sur exemples synthétiques :")
test_predictions.select(
    "transaction_id",
    "amount",
    "country",
    "is_fraud_simulated",
    "prediction",
    "probability"
).show(truncate=False)

# -------------------------------
# 1️⃣4️⃣ Nettoyage
# -------------------------------
spark.stop()
print("\n✨ Modèle de détection de fraude amélioré avec succès !")