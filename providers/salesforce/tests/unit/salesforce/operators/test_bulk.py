# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from unittest.mock import Mock, patch

import pytest

from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.salesforce.exceptions import SalesforceBulkOperationException
from airflow.providers.salesforce.operators.bulk import SalesforceBulkOperator


class TestSalesforceBulkOperator:
    """
    Test class for SalesforceBulkOperator
    """

    def test_execute_missing_operation(self):
        """
        Test execute missing operation
        """
        with pytest.raises((TypeError, AirflowException), match="missing keyword argument 'operation'"):
            SalesforceBulkOperator(
                task_id="no_missing_operation_arg",
                object_name="Account",
                payload=[],
            )

        with pytest.raises(ValueError, match="Operation 'operation' not found!"):
            SalesforceBulkOperator(
                task_id="missing_operation",
                operation="operation",
                object_name="Account",
                payload=[],
            )

    def test_execute_missing_object_name(self):
        """
        Test execute missing object_name
        """
        with pytest.raises((TypeError, AirflowException), match="missing keyword argument 'object_name'"):
            SalesforceBulkOperator(
                task_id="no_object_name_arg",
                operation="insert",
                payload=[],
            )

        with pytest.raises(
            ValueError, match="The required parameter 'object_name' cannot have an empty value."
        ):
            SalesforceBulkOperator(
                task_id="missing_object_name",
                operation="insert",
                object_name="",
                payload=[],
            )

    @patch("airflow.providers.salesforce.operators.bulk.SalesforceHook.get_conn")
    def test_execute_salesforce_bulk_insert(self, mock_get_conn):
        """
        Test execute bulk insert
        """

        operation = "insert"
        object_name = "Account"
        payload = [
            {"Name": "account1"},
            {"Name": "account2"},
        ]
        batch_size = 10000
        use_serial = True

        mock_get_conn.return_value.bulk.__getattr__(object_name).insert = Mock(return_value=[])
        operator = SalesforceBulkOperator(
            task_id="bulk_insert",
            operation=operation,
            object_name=object_name,
            payload=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )

        operator.execute(context={})

        mock_get_conn.return_value.bulk.__getattr__(object_name).insert.assert_called_once_with(
            data=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )

    @patch("airflow.providers.salesforce.operators.bulk.SalesforceHook.get_conn")
    def test_execute_salesforce_bulk_update(self, mock_get_conn):
        """
        Test execute bulk update
        """

        operation = "update"
        object_name = "Account"
        payload = [
            {"Id": "000000000000000AAA", "Name": "account1"},
            {"Id": "000000000000000BBB", "Name": "account2"},
        ]
        batch_size = 10000
        use_serial = True

        mock_get_conn.return_value.bulk.__getattr__(object_name).update = Mock(return_value=[])
        operator = SalesforceBulkOperator(
            task_id="bulk_update",
            operation=operation,
            object_name=object_name,
            payload=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )

        operator.execute(context={})

        mock_get_conn.return_value.bulk.__getattr__(object_name).update.assert_called_once_with(
            data=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )

    @patch("airflow.providers.salesforce.operators.bulk.SalesforceHook.get_conn")
    def test_execute_salesforce_bulk_upsert(self, mock_get_conn):
        """
        Test execute bulk upsert
        """

        operation = "upsert"
        object_name = "Account"
        payload = [
            {"Id": "000000000000000AAA", "Name": "account1"},
            {"Name": "account2"},
        ]
        external_id_field = "Id"
        batch_size = 10000
        use_serial = True

        mock_get_conn.return_value.bulk.__getattr__(object_name).upsert = Mock(return_value=[])
        operator = SalesforceBulkOperator(
            task_id="bulk_upsert",
            operation=operation,
            object_name=object_name,
            payload=payload,
            external_id_field=external_id_field,
            batch_size=batch_size,
            use_serial=use_serial,
        )

        operator.execute(context={})

        mock_get_conn.return_value.bulk.__getattr__(object_name).upsert.assert_called_once_with(
            data=payload,
            external_id_field=external_id_field,
            batch_size=batch_size,
            use_serial=use_serial,
        )

    @patch("airflow.providers.salesforce.operators.bulk.SalesforceHook.get_conn")
    def test_execute_salesforce_bulk_delete(self, mock_get_conn):
        """
        Test execute bulk delete
        """

        operation = "delete"
        object_name = "Account"
        payload = [
            {"Id": "000000000000000AAA"},
            {"Id": "000000000000000BBB"},
        ]
        batch_size = 10000
        use_serial = True

        mock_get_conn.return_value.bulk.__getattr__(object_name).delete = Mock(return_value=[])
        operator = SalesforceBulkOperator(
            task_id="bulk_delete",
            operation=operation,
            object_name=object_name,
            payload=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )

        operator.execute(context={})

        mock_get_conn.return_value.bulk.__getattr__(object_name).delete.assert_called_once_with(
            data=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )

    @patch("airflow.providers.salesforce.operators.bulk.SalesforceHook.get_conn")
    def test_execute_salesforce_bulk_hard_delete(self, mock_get_conn):
        """
        Test execute bulk hard_delete
        """

        operation = "hard_delete"
        object_name = "Account"
        payload = [
            {"Id": "000000000000000AAA"},
            {"Id": "000000000000000BBB"},
        ]
        batch_size = 10000
        use_serial = True

        mock_get_conn.return_value.bulk.__getattr__(object_name).hard_delete = Mock(return_value=[])
        operator = SalesforceBulkOperator(
            task_id="bulk_hard_delete",
            operation=operation,
            object_name=object_name,
            payload=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )

        operator.execute(context={})

        mock_get_conn.return_value.bulk.__getattr__(object_name).hard_delete.assert_called_once_with(
            data=payload,
            batch_size=batch_size,
            use_serial=use_serial,
        )

    def test_check_response_for_errors_all_success(self):
        """
        Test that all successful records do not raise an exception.
        """
        operator = SalesforceBulkOperator(
            task_id="bulk_insert",
            operation="insert",
            object_name="Account",
            payload=[],
        )
        result = [
            {"success": True, "id": "001"},
            {"success": True, "id": "002"},
        ]
        assert operator._check_response_for_errors(result) is None

    @patch("airflow.providers.salesforce.operators.bulk.SalesforceHook.get_conn")
    def test_execute_raises_on_failed_records(self, mock_get_conn):
        """
        Test that execute raises SalesforceBulkOperationException when API returns failures.
        """
        object_name = "Account"
        payload = [{"Name": "account1"}, {"Name": "account2"}]
        failed_record = {
            "success": False,
            "created": False,
            "id": None,
            "errors": [
                {
                    "statusCode": "INVALID_FIELD",
                    "message": "Unexpected JsonMappingException: No such column 'Wrong_Asset_Key'"
                    " on sobject of type Asset",
                    "fields": [],
                }
            ],
        }
        mock_get_conn.return_value.bulk.__getattr__(object_name).insert = Mock(
            return_value=[
                {"success": True, "id": "001"},
                failed_record,
            ]
        )
        operator = SalesforceBulkOperator(
            task_id="bulk_insert",
            operation="insert",
            object_name=object_name,
            payload=payload,
        )
        with pytest.raises(SalesforceBulkOperationException, match="1/2 records") as exc_info:
            operator.execute(context={})
        assert "INVALID_FIELD" in str(exc_info.value)
