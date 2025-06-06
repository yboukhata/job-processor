import { fr } from "@codegouvfr/react-dsfr";
import { Accordion } from "@codegouvfr/react-dsfr/Accordion.js";
import { Box, CircularProgress, Typography } from "@mui/material";
import type { Jsonify } from "type-fest";
import { Alert } from "@codegouvfr/react-dsfr/Alert.js";

import { IJob, ProcessorStatusJson } from "../../common/model.ts";
import { useMemo } from "react";

function formatDate(date: string | null | undefined) {
  if (!date) return " - ";
  return new Date(date).toLocaleString();
}

export function getTaskStatus(task: Jsonify<IJob> | null | undefined) {
  if (!task) return "Jamais exécuté";

  switch (task.status) {
    case SimpleJobStatus.Pending:
      return new Date(task.scheduled_for).getTime() > Date.now()
        ? "Programmé"
        : "En attente";
    case SimpleJobStatus.Running:
      return "En cours";
    case SimpleJobStatus.Finished:
      return "Terminé";
    case SimpleJobStatus.Errored:
      return "Erreur";
    case SimpleJobStatus.Paused:
      return "En pause";
    default:
      return "Inconnu";
  }
}

type ProcessorStatusTaskComponentProps = {
  status: ProcessorStatusJson | null;
  type: JobType.Cron | "job";
  baseUrl: string;
  name: string;
  id: string;
};

export function ProcessorStatusTaskComponent(
  props: ProcessorStatusTaskComponentProps,
) {
  const { status, name, type, id } = props;
  const task = useMemo(() => {
    if (!status) return null;

    if (type === "job") {
      return status.jobs
        .find((job) => job.name === name)
        ?.tasks.find((task) => task._id === id);
    }
    const cronStatus = status.crons.find((cron) => cron.cron.name === name);

    if (!cronStatus) return null;
    const { scheduled, running, history } = cronStatus;

    return (
      scheduled.find((task) => task._id === id) ??
      running.find((task) => task._id === id) ??
      history.find((task) => task._id === id)
    );
  }, [status, name, type, id]);

  if (!status) {
    return (
      <Box
        sx={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          my: fr.spacing("8w"),
        }}
      >
        <CircularProgress />
      </Box>
    );
  }

  if (!task) return <Alert severity="error" title="Non trouvé" />;

  const { output } = task;

  return (
    <Box
      sx={{ display: "flex", flexDirection: "column", gap: fr.spacing("2w") }}
    >
      <Typography>
        Statut: <strong>{getTaskStatus(task)}</strong>
      </Typography>
      <Typography>
        Planifié pour: <strong>{formatDate(task.scheduled_for)}</strong>
      </Typography>
      <Typography>
        Démarré à: <strong>{formatDate(task.started_at)}</strong>
      </Typography>
      <Typography>
        Terminé à: <strong>{formatDate(task.ended_at)}</strong>
      </Typography>
      <Typography>
        Durée: <strong>{task.output?.duration ?? " - "}</strong>
      </Typography>
      {task.type === JobType.Simple && task.payload && (
        <Accordion label="Paramètres">
          <code>
            <pre>{JSON.stringify(task.payload, null, 2)}</pre>
          </code>
        </Accordion>
      )}
      {output && (
        <Accordion label="Résultat">
          <code>
            <pre>{JSON.stringify(output, null, 2)}</pre>
          </code>
        </Accordion>
      )}
    </Box>
  );
}
