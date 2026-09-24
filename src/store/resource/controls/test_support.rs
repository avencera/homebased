//! Seeding helpers for tests that drive resource controls through a running daemon

use std::path::Path;

use rusqlite::params;

use crate::resource::{Loan, SupervisorNotice};
use crate::store::Store;

impl Store {
    /// Insert one non-closed loan and one notice exactly as given, beside a running StoreActor
    pub(crate) fn seed_loan_notice_for_test(
        database: &Path,
        loan: &Loan,
        notice: &SupervisorNotice,
    ) {
        let store = Self::open(database).unwrap();
        store
            .conn
            .execute(
                "INSERT INTO loans (id, resource_id, state_json) VALUES (?1, ?2, ?3)",
                params![
                    loan.id.as_uuid().to_string(),
                    loan.resource_id.as_uuid().to_string(),
                    serde_json::to_string(&loan.state).unwrap(),
                ],
            )
            .unwrap();
        store
            .conn
            .execute(
                "INSERT INTO resource_supervisor_notices (id, loan_id, action_id, notice_json)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    notice.id.as_uuid().to_string(),
                    notice.loan_id.as_uuid().to_string(),
                    notice.action_id.as_uuid().to_string(),
                    serde_json::to_string(notice).unwrap(),
                ],
            )
            .unwrap();
    }

    /// Count committed resource control operations beside a running StoreActor
    pub(crate) fn resource_control_operation_count_for_test(database: &Path) -> i64 {
        Self::open(database)
            .unwrap()
            .conn
            .query_row(
                "SELECT COUNT(*) FROM resource_control_operations",
                [],
                |row| row.get(0),
            )
            .unwrap()
    }
}
