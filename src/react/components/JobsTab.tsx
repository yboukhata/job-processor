import { useMemo } from "react";

import { Table, WrappeDataGridAction } from "./Table.tsx";
import { DsfrLink } from "./DsfrLink.tsx";
import { getTaskStatus } from "./ProcessorStatusTaskComponent.tsx";
import { ProcessorStatusJson } from "../../common/model.ts";

type JobRow = {
  _id: string;
  name: string;
  statut: string;
  scheduled_for: Date | null;
  last_execution_status: string;
  last_execution_duration: string;
  last_execution_date: Date | null;
};

export function JobsTab(
  props: Pick<ProcessorStatusJson, "jobs"> & { baseUrl: string },
) {
  const rows: JobRow[] = useMemo(() => {
    return props.jobs.map((job): JobRow => {
      const lastRun = job.tasks.find(
        (task) => task.status !== SimpleJobStatus.Pending,
      );
      const nextRun = job.tasks.find(
        (task) => task.status === SimpleJobStatus.Pending,
      );

      return {
        _id: job.name,
        name: job.name,
        statut: getTaskStatus(job.tasks[0]),
        scheduled_for: nextRun ? new Date(nextRun.scheduled_for) : null,
        last_execution_status: getTaskStatus(lastRun),
        last_execution_duration: lastRun?.output?.duration ?? "-",
        last_execution_date: lastRun?.started_at
          ? new Date(lastRun.started_at)
          : null,
      };
    });
  }, [props.jobs]);

  return (
    <Table
      getRowId={(row) => row._id}
      rows={rows}
      columns={[
        {
          field: "name",
          headerName: "Nom",
          flex: 1,
        },
        {
          field: "statut",
          headerName: "Statut",
        },
        {
          field: "scheduled_for",
          headerName: "Planifié pour",
          type: "dateTime",
        },
        {
          field: "last_execution_status",
          headerName: "Dernier Statut",
        },
        {
          field: "last_execution_duration",
          headerName: "Durée",
        },
        {
          field: "last_execution_date",
          headerName: "Date de la dernière exécution",
          type: "dateTime",
        },
        {
          field: "actions",
          headerName: "Voir",
          type: "actions",
          getActions: ({ row: { name } }) => [
            <WrappeDataGridAction key="Voir">
              <DsfrLink href={`${props.baseUrl}/job/${name}`} />
            </WrappeDataGridAction>,
          ],
        },
      ]}
    />
  );
}
