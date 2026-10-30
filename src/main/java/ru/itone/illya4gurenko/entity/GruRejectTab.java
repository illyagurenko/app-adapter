package ru.itone.illya4gurenko.entity;

import lombok.Data;
import lombok.experimental.Accessors;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * сущность таблицы забракованных записей GRU_REJECT_TAB.
 */
@Data
@Accessors(chain = true)
public class GruRejectTab {

    private Long id;

    private String systemAccount;

    private Long vistaTabId;

    private Long uterrario;

    private BigDecimal oldTBal;

    private BigDecimal newTBal;

    private LocalDateTime frontTimestamp;

    private String frontStatus;

    private String rejectDesc;

    private String checkStatus;

    private String checkDesc;

    private String checkUser;

    private LocalDateTime checkTimestamp;

    private String svfeLoadId;

}