# EcommerceAnalytics

## 1. Présentation du projet

EcommerceAnalytics est un projet d'analyse de données e-commerce développé en Scala avec Apache Spark. Il a pour objectif de charger, traiter et analyser des données de transactions commerciales.

## 2. Technologies utilisées

- Scala 2.13.14
- Apache Spark 3.5.1
- sbt pour la gestion des dépendances et la compilation
- Typesafe Config pour la gestion de la configuration

## 3. Prérequis

Avant d'exécuter le projet, installer et configurer :

- un JDK compatible avec la version de Spark utilisée ;
- sbt 1.x ;
- les dépendances déclarées dans `build.sbt` ;
- Apache Spark, si nécessaire pour l'exécution distribuée avec `spark-submit`.

Vérifier les installations :

```bash
java -version
sbt --version
spark-submit --version
```

## 4. Structure du projet

```text
EcommerceAnalytics/
├── build.sbt
├── project/
├── src/
│   └── main/
│       ├── scala/
│       │   └── com/ecommerce/
│       │       ├── analytics/
│       │       ├── models/
│       │       └── utils/
│       └── resources/
│           └── application.conf
├── data/
├── README.md
└── EQUIPE.md
```

## 5. Compilation

Depuis la racine du projet :

```bash
sbt clean
sbt compile
```

## 6. Exécution avec sbt

```bash
sbt run
```

Cette commande lance le point d'entrée configuré dans le projet, si celui-ci est correctement défini.

## 7. Génération du JAR

```bash
sbt assembly
```

Le JAR est normalement généré dans le dossier `target/scala-2.13/`.

## 8. Exécution local

Après avoir généré le JAR :


Pour lancer l'application avec SBT :
```bash
 sbt "runMain com.ecommerce.analytics.MainApp" 
```

L'application utilise Spark en mode local, configuré avec local[*].

## 9. Déploiement avec spark-submit
Pour lancer le jar localement:
```bash
spark-submit --class com.ecommerce.analytics.MainApp --master "local[*]" target/scala-2.13/EcommerceAnalytics.jar
```

Pour un cluster Spark Standalone, adapter la commande suivante à l'environnement de déploiement :
```bash
spark-submit --class com.ecommerce.analytics.MainApp --master spark://ADRESSE_DU_MASTER:7077 --deploy-mode client target/scala-2.13/EcommerceAnalytics.jar
```
## 9. Configuration et données

Les paramètres de l'application sont définis dans `src/main/resources/application.conf`. Les fichiers de données sont placés dans le répertoire prévu par la configuration du projet.

Les chemins de fichiers doivent être vérifiés avant l'exécution.

## 10. Équipe

Les informations relatives aux membres et à la répartition du travail sont présentées dans `EQUIPE.md`.