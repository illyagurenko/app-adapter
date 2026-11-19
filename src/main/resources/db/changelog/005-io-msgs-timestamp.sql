--liquibase formatted sql

--changeset illya:adapter-017-io-msgs-ins-ts-timestamp failOnError:false
ALTER TABLE ACC_APP_ADAPTER.APP_ADAPTER_IO_MSGS MODIFY (INS_TS TIMESTAMP(3));

--rollback ALTER TABLE ACC_APP_ADAPTER.APP_ADAPTER_IO_MSGS MODIFY (INS_TS DATE);
