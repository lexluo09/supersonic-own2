import { useState } from 'react';
import type { FC } from 'react';
import { Select, message } from 'antd';
import type { UserItem } from './service';
import { getAllUser } from './service';

import styles from './index.less';
import { useFetchDataEffect } from '@/utils/curd';
import TMEAvatar from '../TMEAvatar';

interface Props {
  value?: string[];
  placeholder?: string;
  isMultiple?: boolean;
  onChange?: (owners: string | string[]) => void;
}

const SelectTMEPerson: FC<Props> = ({ placeholder, value, isMultiple = true, onChange }) => {
  const [userList, setUserList] = useState<UserItem[]>([]);

  useFetchDataEffect(
    {
      fetcher: async () => {
        const res = await getAllUser();
        if (res.code !== 200) {
          message.error(res.msg);
          throw new Error(res.msg);
        }
        return res.data || [];
      },
      updater: (list) => {
        const users = list.map((item: UserItem) => {
          const { enName, chName, name } = item;
          return {
            ...item,
            enName: enName || name,
            chName: chName || name,
          };
        });
        setUserList(users);
      },
      cleanup: () => {
        setUserList([]);
      },
    },
    [],
  );

  return (
    <Select
      value={value}
      placeholder={placeholder ?? '请选择用户名'}
      mode={isMultiple ? 'multiple' : undefined}
      allowClear
      showSearch
      onChange={onChange}
    >
      {userList.map((item) => {
        return (
          <Select.Option key={item.enName} value={item.enName}>
            <TMEAvatar size="small" staffName={item.enName} />
            <span className={styles.userText}>{item.displayName}</span>
          </Select.Option>
        );
      })}
    </Select>
  );
};

export default SelectTMEPerson;
