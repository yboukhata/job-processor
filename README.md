# Job processor

Le `job processor` est un service conçu pour traiter des tâches en arrière-plan de manière efficace et fiable.

> **Ce service supporte à la fois MongoDB et PostgreSQL comme moteur de stockage.**  
> Vous pouvez choisir le moteur de base de données le plus adapté à votre environnement en configurant l'option `databaseType` à `"mongo"` ou `"postgres"` lors de l'initialisation.

Il a été développé pour le template apprentissage [Template Apprentissage](https://github.com/mission-apprentissage/template-apprentissage) et est déjà installé et pré-configuré sur celui-ci.

## Installation

```bash
yarn add job-processor
```

## Configuration

Le job processor s'initialise avec l'import de la fonction `initJobProcessor()` :

```js
  initJobProcessor({
    databaseType: "mongo" | "postgres",
    db: Db,
    logger: ILogger,
    jobs: Record<string, JobDef>,
    crons: Record<string, CronDef>,
    workerTags?: string[] | null
  })
```

## Options

- **databaseType** : Type de base de données à utiliser (`"mongo"` ou `"postgres"`).
- **db** : Connecteur de base de données MongoDB ou PostgreSQL.
- **logger** : Instance de logging, `bunyan` est utilisé par défaut sur le template.
- **jobs** : Liste des tâches à exécuter.
- **crons** : Liste des tâches CRONs à exécuter.
- **workerTags** : Dans le cas où l'utilisateur souhaite attribuer une ou plusieurs tâches à un réplica spécifique, il convient de fournir la liste des réplicas (voir configuration Worker Tags).

### Job options

Les options des jobs permettent de configurer le comportement de chaque tâche individuellement.

#### `JobDef`

```ts
type JobDef = {
  handler: (job: IJobsSimple, signal: AbortSignal) => Promise<unknown>;
  onJobExited?: (job: IJobsSimple) => Promise<unknown>;
  resumable?: boolean;
  tag?: string | null;
};
```

- **handler** : Fonction asynchrone qui exécute la tâche. Reçoit le job et un signal d'annulation.
- **onJobExited** : Fonction asynchrone appelée lorsque la tâche se termine, avec le dernier traitement du job en paramètre. Cette méthode est également appelée en cas de crash.
- **resumable** : Indique si la tâche peut être reprise après un redémarrage (par défaut: `false`).
- **tag** : Une chaîne permettant d'attribuer un job à un worker spécifique (par défaut: `null`).

#### `CronDef`

```ts
type CronDef = {
  cron_string: string;
  handler: (signal: AbortSignal) => Promise<unknown>;
  onJobExited?: (job: IJobsCronTask) => Promise<unknown>;
  resumable?: boolean;
  checkinMargin?: number;
  maxRuntimeInMinutes?: number;
  tag?: string | null;
};
```

- **cron_string** : Expression CRON définissant la fréquence d'exécution de la tâche.
- **handler** : Fonction asynchrone exécutant la tâche selon la planification.
- **onJobExited** : Fonction asynchrone appelée lorsque la tâche se termine, avec le dernier traitement du job en paramètre. Cette méthode est également appelée en cas de crash.
- **resumable** : Indique si la tâche CRON peut être reprise après un redémarrage.
- **checkinMargin**: Tolérance en minutes pour le délai entre l'heure planifiée et l'heure d'exécution effective.
- **maxRuntimeInMinutes** : Durée maximale d'exécution avant interruption forcée.
- **tag** : Une chaîne permettant d'attribuer une tâche à un worker spécifique.

## Configuration Worker Tags

L'ajout de `workerTags` permet d'attribuer une ou plusieurs tâches à un worker en particulier.
Par défaut, toutes les tâches peuvent être attribuées à l'ensemble des workers disponibles.

Ajuster la configuration de l'application :

- Dans le fichier `docker-compose.production.yml`, ajouter la variable d'environnement au service `job-processor` :

```yaml
environment:
  INSTANCE_ID: "runner-{% raw %}{{.Task.Slot}}{% endraw %}"
```

- Récupérer la variable depuis le fichier `config.ts`

```ts
    worker: env.get("INSTANCE_ID").asString(),
```

- Intégrer la gestion des workers dans la fonction `initJobProcessor()` :

```ts
workerTags: config.worker === "runner-1" ? ["main"] : ["slave"];
```

Vous pouvez désormais attribuer un tag à un job :

```ts
jobs: {
  "my-super-job":{
    handler: fn,
    tag: "main"
  }
}
```
