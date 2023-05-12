-- Project      Control Freemium Dev
-- Purpose      Used to get instance level features aggregation
-- Input        - Tables:
--                      -- airflow.instanceaggregation
-- Database     - Redshift
-----------------------------------------------------------------------------------------------------------
SELECT
    instanceid,
    DATE(DATE_TRUNC('MONTH', DATE)),
    SUM(uniquesessionsconnectedbyhosts_windowsapp_count) AS uniquesessionsconnectedbyhosts_windowsapp_count,
    SUM(
        uniquesessionsconnectedbyhosts_monotouchios_count
    ) AS uniquesessionsconnectedbyhosts_monotouchios_count,
    SUM(uniquesessionsconnectedbyhosts_macapp_count) AS uniquesessionsconnectedbyhosts_macapp_count,
    SUM(uniquesessionsconnectedbyhosts_javaswing_count) AS uniquesessionsconnectedbyhosts_javaswing_count,
    SUM(uniquesessionsconnectedbyhosts_javaandroid_count) AS uniquesessionsconnectedbyhosts_javaandroid_count,
    SUM(
        uniquesessionsconnectedbyhosts_dotnetwinforms_count
    ) AS uniquesessionsconnectedbyhosts_dotnetwinforms_count,
    SUM(uniquesessionsconnectedbyguests_windowsapp_count) AS uniquesessionsconnectedbyguests_windowsapp_count,
    SUM(
        uniquesessionsconnectedbyguests_monotouchios_count
    ) AS uniquesessionsconnectedbyguests_monotouchios_count,
    SUM(uniquesessionsconnectedbyguests_macapp_count) AS uniquesessionsconnectedbyguests_macapp_count,
    SUM(uniquesessionsconnectedbyguests_javaswing_count) AS uniquesessionsconnectedbyguests_javaswing_count,
    SUM(
        uniquesessionsconnectedbyguests_javaandroid_count
    ) AS uniquesessionsconnectedbyguests_javaandroid_count,
    SUM(
        uniquesessionsconnectedbyguests_dotnetwinforms_count
    ) AS uniquesessionsconnectedbyguests_dotnetwinforms_count,
    SUM(sessionevent_sentmessage_count) AS sessionevent_sentmessage_count,
    SUM(sessionevent_queuedwake_count) AS sessionevent_queuedwake_count,
    SUM(sessionevent_queueduninstallandend_count) AS sessionevent_queueduninstallandend_count,
    SUM(sessionevent_queueduninstall_count) AS sessionevent_queueduninstall_count,
    SUM(sessionevent_queuedtool_count) AS sessionevent_queuedtool_count,
    SUM(sessionevent_queuedreinstall_count) AS sessionevent_queuedreinstall_count,
    SUM(sessionevent_queuedmessage_count) AS sessionevent_queuedmessage_count,
    SUM(sessionevent_queuedinvalidatelicense_count) AS sessionevent_queuedinvalidatelicense_count,
    SUM(sessionevent_queuedinstallaccess_count) AS sessionevent_queuedinstallaccess_count,
    SUM(sessionevent_queuedguestinfoupdate_count) AS sessionevent_queuedguestinfoupdate_count,
    SUM(sessionevent_queuedelevatedtool_count) AS sessionevent_queuedelevatedtool_count,
    SUM(sessionevent_queuedcommand_count) AS sessionevent_queuedcommand_count,
    SUM(sessionevent_none_count) AS sessionevent_none_count,
    SUM(sessionevent_modifiedname_count) AS sessionevent_modifiedname_count,
    SUM(sessionevent_modifiedispublic_count) AS sessionevent_modifiedispublic_count,
    SUM(sessionevent_modifiedhost_count) AS sessionevent_modifiedhost_count,
    SUM(sessionevent_modifiedcustomproperty_count) AS sessionevent_modifiedcustomproperty_count,
    SUM(sessionevent_modifiedcode_count) AS sessionevent_modifiedcode_count,
    SUM(sessionevent_initiatedjoin_count) AS sessionevent_initiatedjoin_count,
    SUM(sessionevent_endedsession_count) AS sessionevent_endedsession_count,
    SUM(sessionevent_createdsession_count) AS sessionevent_createdsession_count,
    SUM(sessionevent_addednote_count) AS sessionevent_addednote_count,
    SUM(
        sessionconnectionevent_switchedlogonsession_count
    ) AS sessionconnectionevent_switchedlogonsession_count,
    SUM(sessionconnectionevent_sentprintjob_count) AS sessionconnectionevent_sentprintjob_count,
    SUM(sessionconnectionevent_sentmessage_count) AS sessionconnectionevent_sentmessage_count,
    SUM(sessionconnectionevent_sentfiles_count) AS sessionconnectionevent_sentfiles_count,
    SUM(sessionconnectionevent_receivedprintjob_count) AS sessionconnectionevent_receivedprintjob_count,
    SUM(sessionconnectionevent_ranfiles_count) AS sessionconnectionevent_ranfiles_count,
    SUM(sessionconnectionevent_rancommand_count) AS sessionconnectionevent_rancommand_count,
    SUM(
        sessionconnectionevent_queuedforcedisconnect_count
    ) AS sessionconnectionevent_queuedforcedisconnect_count,
    SUM(sessionconnectionevent_endedsession_count) AS sessionconnectionevent_endedsession_count,
    SUM(sessionconnectionevent_draggedfiles_count) AS sessionconnectionevent_draggedfiles_count,
    SUM(sessionconnectionevent_disconnected_count) AS sessionconnectionevent_disconnected_count,
    SUM(sessionconnectionevent_copiedfiles_count) AS sessionconnectionevent_copiedfiles_count,
    SUM(sessionconnectionevent_connected_count) AS sessionconnectionevent_connected_count,
    SUM(loginresult_usernameinvalid_count) AS loginresult_usernameinvalid_count,
    SUM(loginresult_unchangeablepasswordexpired_count) AS loginresult_unchangeablepasswordexpired_count,
    SUM(loginresult_success_count) AS loginresult_success_count,
    SUM(loginresult_passwordinvalid_count) AS loginresult_passwordinvalid_count,
    SUM(loginresult_onetimepasswordinvalid_count) AS loginresult_onetimepasswordinvalid_count,
    SUM(loginresult_lockedout_count) AS loginresult_lockedout_count,
    SUM(loginresult_changeablepasswordexpired_count) AS loginresult_changeablepasswordexpired_count,
    SUM(hostconnectionclienttype_windowsapp_count) AS hostconnectionclienttype_windowsapp_count,
    SUM(hostconnectionclienttype_monotouchios_count) AS hostconnectionclienttype_monotouchios_count,
    SUM(hostconnectionclienttype_macapp_count) AS hostconnectionclienttype_macapp_count,
    SUM(hostconnectionclienttype_javaswing_count) AS hostconnectionclienttype_javaswing_count,
    SUM(hostconnectionclienttype_javaandroid_count) AS hostconnectionclienttype_javaandroid_count,
    SUM(hostconnectionclienttype_dotnetwinforms_count) AS hostconnectionclienttype_dotnetwinforms_count,
    SUM(guestconnectionclienttype_windowsapp_count) AS guestconnectionclienttype_windowsapp_count,
    SUM(guestconnectionclienttype_monotouchios_count) AS guestconnectionclienttype_monotouchios_count,
    SUM(guestconnectionclienttype_macapp_count) AS guestconnectionclienttype_macapp_count,
    SUM(guestconnectionclienttype_javaswing_count) AS guestconnectionclienttype_javaswing_count,
    SUM(guestconnectionclienttype_javaandroid_count) AS guestconnectionclienttype_javaandroid_count,
    SUM(guestconnectionclienttype_dotnetwinforms_count) AS guestconnectionclienttype_dotnetwinforms_count,
    SUM(createsession_support_count) AS createsession_support_count,
    SUM(createsession_meeting_count) AS createsession_meeting_count,
    SUM(createsession_access_count) AS createsession_access_count
FROM
    airflow.instanceaggregation
WHERE
    DATE > '2021-10-01'
GROUP BY
    1,
    2