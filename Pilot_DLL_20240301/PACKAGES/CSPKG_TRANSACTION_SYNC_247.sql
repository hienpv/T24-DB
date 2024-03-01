--------------------------------------------------------
--  DDL for Package CSPKG_TRANSACTION_SYNC_247
--------------------------------------------------------

  CREATE OR REPLACE EDITIONABLE PACKAGE "CSPKG_TRANSACTION_SYNC_247" AS
  /** author: QuangBD3
  **/
  PROCEDURE pr_tmtran_fail_later_process;

  PROCEDURE pr_tmtran_sync(dm_operation_type      in CHAR,
                           orchestrate_tmtxcd     in VARCHAR,
                           orchestrate_tmresv07   in VARCHAR,
                           orchestrate_tmdorc     in CHAR,
                           orchestrate_tmtxamt    in NUMBER,
                           orchestrate_tmglcur    in VARCHAR,
                           orchestrate_tmorgamt   in NUMBER,
                           orchestrate_tmefth     in VARCHAR2,
                           orchestrate_tmacctno   in NUMBER,
                           orchestrate_tmtellid   in VARCHAR,
                           orchestrate_tmtxseq    in NUMBER,
                           orchestrate_tmtxstat   in VARCHAR,
                           orchestrate_tmhosttxcd in NUMBER,
                           orchestrate_tmapptype  in CHAR,
                           orchestrate_tmeqvtrn   in CHAR,
                           orchestrate_tmibttrn   in CHAR,
                           orchestrate_tmsumtrn   in CHAR,
                           orchestrate_tmentdt7   in NUMBER,
                           orchestrate_tmeffdt7   in NUMBER,
                           orchestrate_tmsseq     in NUMBER,
                           orchestrate_tmtiment   in NUMBER);

  PROCEDURE pr_tmtran_fail_sync(dm_operation_type      in CHAR,
                                orchestrate_tmtxcd     in VARCHAR,
                                orchestrate_tmresv07   in VARCHAR,
                                orchestrate_tmdorc     in CHAR,
                                orchestrate_tmtxamt    in NUMBER,
                                orchestrate_tmglcur    in VARCHAR,
                                orchestrate_tmorgamt   in NUMBER,
                                orchestrate_tmefth     in VARCHAR2,
                                orchestrate_tmacctno   in NUMBER,
                                orchestrate_tmtellid   in VARCHAR,
                                orchestrate_tmtxseq    in NUMBER,
                                orchestrate_tmtxstat   in VARCHAR,
                                orchestrate_tmhosttxcd in NUMBER,
                                orchestrate_tmapptype  in CHAR,
                                orchestrate_tmeqvtrn   in CHAR,
                                orchestrate_tmibttrn   in CHAR,
                                orchestrate_tmsumtrn   in CHAR,
                                orchestrate_tmentdt7   in NUMBER,
                                orchestrate_tmeffdt7   in NUMBER,
                                orchestrate_tmsseq     in NUMBER,
                                orchestrate_tmtiment   in NUMBER);

END CSPKG_TRANSACTION_SYNC_247;

/
