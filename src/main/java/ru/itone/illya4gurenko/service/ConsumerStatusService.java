package ru.itone.illya4gurenko.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.itone.illya4gurenko.dao.AppAdapterTransDao;
import ru.itone.illya4gurenko.entity.AppAdapterTrans;
import ru.itone.illya4gurenko.entity.enums.FocStatus;

@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumerStatusService {

    private final AppAdapterTransDao transDao;


    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markInProcess(AppAdapterTrans trans) {
        trans.setStatus(FocStatus.CONSUMER_IN_PROCESS)
                .setRespDesc("consumer processing response");

        transDao.updateStatus(trans);

        log.info("transaction requestId={} moved to {}",
                trans.getRequestId(),
                FocStatus.CONSUMER_IN_PROCESS
        );
    }
}