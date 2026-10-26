
### Justification du choix de génération du JAR exécutable

Pour ce projet, le groupe a choisi d'utiliser l'approche **Assembly** (via le plugin `sbt-assembly`) plutôt qu'un simple `sbt package`.

**Pourquoi ce choix ?**
1. **Autonomie et Portabilité (Fat JAR) :** Contrairement à `sbt package` qui ne compile que notre code local, `sbt assembly` embarque toutes les dépendances externes (comme les librairies de parsing ou de configuration) à l'intérieur d'un unique fichier JAR "obèse". Le livrable peut ainsi être exécuté sur n'importe quelle machine ou cluster (par exemple Spark) sans nécessiter l'installation préalable des bibliothèques sur le serveur cible.
2. **Exécutable direct :** En configurant explicitement la `mainClass` dans notre `build.sbt`, l'archive générée est directement exécutable via une simple commande `java -jar`, ce qui simplifie grandement la mise en production et l'intégration continue.
3. **Résolution des conflits :** Le mécanisme de `assemblyMergeStrategy` nous permet de gérer proprement les doublons de fichiers de ressources ou de métadonnées (comme `application.conf`), assurant la stabilité de l'application à l'exécution.
 
## Membre A (Data Ingestion et Plateform Engineer)

### Questions traitées 

J'ai traité la partie 1,2 et 7.

## Difficultés rencontrés:
1. La compatibilité des versions de scala, sbt.
2. 

## Membre B (Data Transformation Engineer)

### Questions traitées

J'ai traité la Partie 3 : l'UDF extractTimeFeatures (3.1), la fonction enrichTransactionData (3.2), les fonctions de fenêtrage (3.3) et le bonus sur les transactions suspectes (3.4). Mon code est dans TimeFeatures.scala et DataTransformation.scala. J'ai aussi écrit un petit programme de démonstration, TransformationDemo.scala, rangé dans src/test pour qu'il ne soit pas dans le JAR. Mon code est relu par le Membre A.

### Charge de travail

[à compléter : nombre d'heures par question]

### Difficultés rencontrées

1. Les timestamps invalides. 700 transactions sur 138 047 ont un timestamp vide, "NA", ou de 12 ou 16 caractères. Il fallait que l'UDF ne fasse pas échouer le job, et qu'elle refuse une date impossible comme le 30 février au lieu de la corriger toute seule.
2. La fenêtre d'activité sur 7 jours. Ma première version utilisait une fenêtre de 7 fois 24 heures, et je trouvais jusqu'à 8 jours distincts sur une période de 7 jours, ce qui n'est pas possible. Je suis passée à une fenêtre en jours calendaires, et le maximum est maintenant de 7.
3. Les colonnes qui portent le même nom dans plusieurs tables (category, name, city, merchant_id). Après les jointures, Spark ne savait plus laquelle utiliser, donc j'ai renommé les colonnes avant de joindre.

### Choix des jointures (Question 3.2)

Je pars de la table des transactions, et je lui ajoute les trois autres tables avec un LEFT JOIN, sur la clé de chaque table. J'ai choisi LEFT partout pour ne perdre aucune transaction.

La table des transactions est la table de départ : elle contient les ventes, et toutes doivent rester dans le résultat.

Pour la table users, la jointure se fait sur user_id. 400 transactions ont un utilisateur qui n'existe pas. Avec un INNER JOIN elles disparaîtraient et le chiffre d'affaires serait faux.

Pour la table products, la jointure se fait sur product_id. 1 546 transactions ont un produit qui n'existe pas, pour la même raison.

Pour la table merchants, la jointure se fait sur merchant_id. 250 transactions ont un marchand qui n'existe pas. J'utilise le merchant_id de la transaction, pas celui de la table produits.

J'ai vérifié que le nombre de lignes ne change pas : 138 047 avant et après les jointures. Cela marche parce que les clés sont uniques dans les trois autres tables (12 000 utilisateurs, 6 000 produits, 600 marchands).

### Décisions techniques

1. Jointures LEFT depuis les transactions : on ne perd aucune transaction, et celles dont la référence est inconnue gardent des valeurs nulles.
2. Un timestamp invalide donne null, pas une valeur par défaut. L'UDF renvoie null quand le timestamp est vide, mal formé ou impossible. Une valeur par défaut fausserait les statistiques, alors qu'un null peut être compté ou filtré.
3. Le sujet ne dit rien de minuit à 6h pour la période "Night" : je l'ai fait aller de 22h à 5h59, pour ne laisser aucune heure sans catégorie. Pour les heures de travail, j'ai pris de 9h00 à 16h59, comme pour les autres périodes (début inclus, fin exclue).
4. Pour la tranche d'âge, l'âge de 25 ans n'est dans aucune tranche du sujet (Jeune : moins de 25 ans, Adulte : 26 à 44 ans). J'ai classé 25 ans dans "Adulte". Les tranches sont donc : moins de 25, de 25 à 44, de 45 à 64, et 65 et plus.
5. Pour le montant cumulé sur 7 jours, j'utilise les 7 x 24 heures qui précèdent la transaction. Pour savoir si un utilisateur est actif, je compte les jours distincts sur 7 jours calendaires. Le délai entre deux achats est en jours entiers.
6. Pour le rang d'une transaction par utilisateur, j'ai pris row_number plutôt que rank, avec l'identifiant de la transaction pour départager deux achats à la même seconde. Chaque transaction a ainsi un rang unique.
7. Pour le bonus 3.4, le panier moyen d'un utilisateur est la moyenne de toutes ses transactions. Une transaction est suspecte si au moins deux conditions sur quatre sont vraies : montant supérieur de plus de 300 % au panier moyen, période Night, moins de 5 minutes depuis l'achat précédent, paiement en CRYPTO. J'obtiens 3 060 transactions suspectes sur 138 047.
8. La taille de la fenêtre (7 jours) et le seuil d'activité (5 jours) sont des paramètres de la classe DataTransformation, avec des valeurs par défaut, pour ne rien coder en dur dans les calculs.
