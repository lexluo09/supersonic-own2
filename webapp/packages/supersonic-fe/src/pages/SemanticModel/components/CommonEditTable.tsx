import type { ActionType } from '@ant-design/pro-table';
import type { Ref, ReactNode } from 'react';
import { Space, message } from 'antd';
import React, { useRef, forwardRef, useImperativeHandle, useState, useEffect } from 'react';

import { EditableProTable } from '@ant-design/pro-table';

type Props = {
  title?: string;
  tableDataSource: any[];
  columnList: any[];
  rowKey: string;
  editableProTableProps?: any;
  onDataSourceChange?: (dataSource: any) => void;
  extenderCtrlColumn?: (text, record, _, action) => ReactNode[];
  editableActionRender?: (row, config, defaultDom, actionRef) => ReactNode[];
  ref?: any;
};

export type CommonEditTableRef = {
  getCommonEditTableDataSource: () => void;
  editTableActionRef: ActionType;
};
const CommonEditTable: React.FC<Props> = forwardRef(
  (
    {
      title,
      columnList,
      rowKey,
      tableDataSource,
      editableProTableProps = {},
      onDataSourceChange,
      extenderCtrlColumn,
      editableActionRender,
    }: Props,
    ref: Ref<any>,
  ) => {
    const [dataSource, setDataSource] = useState<any[]>(tableDataSource);
    const actionRef = useRef<ActionType>();

    useImperativeHandle(ref, () => ({
      getCommonEditTableDataSource: () => {
        return [...dataSource];
      },
      editTableActionRef: actionRef,
    }));

    useEffect(() => {
      setDataSource(
        tableDataSource.map((item: any) => {
          return {
            ...item,
            editRowId: item[rowKey] || (Math.random() * 1000000).toFixed(0),
          };
        }),
      );
    }, [tableDataSource]);

    const handleDataSourceChange = (data: any) => {
      setTimeout(() => {
        onDataSourceChange?.(data);
      }, 0);
    };

    const columns = [
      ...columnList,
      {
        title: '操作',
        dataIndex: 'x',
        valueType: 'option',
        render: (text, record, _, action) => {
          return (
            <Space>
              <a
                key="editable"
                onClick={() => {
                  action?.startEditable?.(record.editRowId);
                }}
              >
                编辑
              </a>
              <a
                key="deleteBtn"
                onClick={() => {
                  const data = [...dataSource].filter((item) => item[rowKey] !== record[rowKey]);
                  setDataSource(data);
                  handleDataSourceChange(data);
                }}
              >
                删除
              </a>
              {extenderCtrlColumn?.(text, record, _, action)}
            </Space>
          );
        },
      },
      {
        dataIndex: 'editRowId',
        hideInTable: true,
      },
    ];

    const defaultActionRender = (row, config, defaultDom) => {
      return editableActionRender?.(row, config, defaultDom, actionRef);
    };
    const actionRender = editableActionRender ? defaultActionRender : undefined;

    return (
      <>
        <EditableProTable
          key={title}
          actionRef={actionRef}
          headerTitle={title}
          rowKey={'editRowId'}
          columns={columns}
          value={dataSource}
          tableAlertRender={() => {
            return false;
          }}
          onChange={(data) => {
            let tableData = data;
            if (rowKey) {
              // 如果rowKey存在，将rowId复写为rowKey值
              tableData = data.map((item: any) => {
                return {
                  ...item,
                  editRowId: item[rowKey],
                };
              });
            }
            setDataSource(tableData);
            handleDataSourceChange(data);
          }}
          editable={{
            onSave: (_, row) => {
              const rowKeyValue = row[rowKey];
              const isSame = dataSource.filter((item: any, index: number) => {
                return index !== row.index && item[rowKey] === rowKeyValue;
              });
              if (isSame[0]) {
                message.error('存在重复值');
                return Promise.reject();
              }
              return true;
            },
            actionRender: actionRender,
          }}
          pagination={false}
          size="small"
          recordCreatorProps={{
            record: () => ({ editRowId: (Math.random() * 1000000).toFixed(0) }),
          }}
          {...editableProTableProps}
        />
      </>
    );
  },
);
export default CommonEditTable;
