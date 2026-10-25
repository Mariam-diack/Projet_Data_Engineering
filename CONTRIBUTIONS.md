
### Justification du choix de génération du JAR exécutable

Pour ce projet, le groupe a choisi d'utiliser l'approche **Assembly** (via le plugin `sbt-assembly`) plutôt qu'un simple `sbt package`.

**Pourquoi ce choix ?**
1. **Autonomie et Portabilité (Fat JAR) :** Contrairement à `sbt package` qui ne compile que notre code local, `sbt assembly` embarque toutes les dépendances externes (comme les librairies de parsing ou de configuration) à l'intérieur d'un unique fichier JAR "obèse". Le livrable peut ainsi être exécuté sur n'importe quelle machine ou cluster (par exemple Spark) sans nécessiter l'installation préalable des bibliothèques sur le serveur cible.
2. **Exécutable direct :** En configurant explicitement la `mainClass` dans notre `build.sbt`, l'archive générée est directement exécutable via une simple commande `java -jar`, ce qui simplifie grandement la mise en production et l'intégration continue.
3. **Résolution des conflits :** Le mécanisme de `assemblyMergeStrategy` nous permet de gérer proprement les doublons de fichiers de ressources ou de métadonnées (comme `application.conf`), assurant la stabilité de l'application à l'exécution.
 