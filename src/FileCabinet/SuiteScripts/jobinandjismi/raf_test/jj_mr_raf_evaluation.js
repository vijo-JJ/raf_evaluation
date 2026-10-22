/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 */

/******************************************************************************
 * Company      : ABC Traders
 * Script Name  : jj_mr_raf_evaluation.js
 * Script Type  : MapReduceScript
 *
 * Description:
 * Create a script that group all the overdue invoices of customers whose duedate was 30 or more days ago ,send an email with pdf and the invoice details as   table ,
 * if the customer got the email in last past 7 days email shouldnt send again.
 *
 * Author       : Jobin and Jismi IT Services
 * Created Date : 22-Oct-2026
 *
 * Revision History
 * ---------------------------------------------------------------------------
 * Version  Date         Author   Remarks
 * ---------------------------------------------------------------------------
 * 1.0      22-Oct-2026  JJI0045  Initial version
 * 2.1      22-Oct-2026  JJI0045  ABC-5 enhancements
 ******************************************************************************/


define(['N/email', 'N/render', 'N/search'],
    /**
 * @param{email} email
 * @param{render} render
 * @param{search} search
 */
    (email, render, search) => {
        /**
         * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
         * @param {Object} inputContext
         * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Object} inputContext.ObjectRef - Object that references the input data
         * @typedef {Object} ObjectRef
         * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
         * @property {string} ObjectRef.type - Type of the record instance that contains the input data
         * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
         * @since 2015.2
         */

        const getInputData = (inputContext) => {
            try{
                return getInvoiceSearch()
            }
            catch(e){
                log.debug({
                    title:e.name,
                    details:e.message
                });
            }
        }
        //search through the database and return necessary details back
        function getInvoiceSearch(){
            return search.create({
                        type: "invoice",
                        settings:[{"name":"consolidationtype","value":"ACCTTYPE"},{"name":"includeperiodendtransactions","value":"F"}],
                        filters:
                        [
                            ["type","anyof","CustInvc"], 
                            "AND", 
                            ["mainline","is","T"], 
                            "AND", 
                            ["amountremaining","greaterthan","0.00"], 
                            "AND", 
                            ["duedate","before","thirtydaysago"], 
                            "AND", 
                            ["status","anyof","CustInvc:A"],
                            "AND", 
                            ["customer.isinactive","is","F"], 
                            "AND", 
                            ["customer.email","isnotempty",""]
                        ],
                        columns:
                        [
                            search.createColumn({name: "entity", label: "Name"}),
                            search.createColumn({name: "tranid", label: "Document Number"}),
                            search.createColumn({name: "internalid", label: "Internal Number"}),
                            search.createColumn({name: "duedate", label: "Due Date/Receive By"}),
                            search.createColumn({name: "amount", label: "Amount"}),
                            search.createColumn({name: "trandate", label: "Date"}),
                            search.createColumn({name: "amountremaining", label: "Amount Remaining"}),
                            search.createColumn({name: "daysoverdue", label: "Days Overdue"})
                        ]
                        });
        }



        /**
         * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
         * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
         * context.
         * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
         *     is provided automatically based on the results of the getInputData stage.
         * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
         *     function on the current key-value pair
         * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
         *     pair
         * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} mapContext.key - Key to be processed during the map stage
         * @param {string} mapContext.value - Value to be processed during the map stage
         * @since 2015.2
         */

        const map = (mapContext) => {
            try{
                let mapData = getData(mapContext)
                writeData(mapContext,mapData)
            }
            catch(e){
                log.debug({
                    title:e.name,
                    details:e.message
                });
            }
        }
        // returns the needed data as variables for further use
        function getData(mapContext){
            let result = JSON.parse(mapContext.value);
            let custName = result.values.entity.text;
            let custId = result.values.entity.value;
            let invId = result.values.tranid;
            let internalId = result.id;
            let dueDate = result.values.duedate;
            let totalAmount = result.values.amount;
            let invDate = result.values.trandate;
            let amountRemaining = result.values.amountremaining;
            let daysOverdue = result.values.daysoverdue;
            
            return{
                custName,custId,invId,internalId,dueDate,totalAmount,invDate,amountRemaining,daysOverdue
            }
        }

        // write the data as key value pair. Customer Id is the key and every other values are passed
        // as its values as a single string
        function writeData(mapContext,mapData){
            mapContext.write({
                key: mapData.custId,
                value:JSON.stringify({
                        custName: mapData.custName,
                        invId:mapData.invId,
                        internalId:mapData.internalId,
                        dueDate:mapData.dueDate,
                        totalAmount:mapData.totalAmount,
                        invDate:mapData.invDate,
                        amountRemaining:mapData.amountRemaining,
                        daysOverdue:mapData.daysOverdue


                })
            });

        }

        /**
         * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
         * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
         * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
         *     provided automatically based on the results of the map stage.
         * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
         *     reduce function on the current group
         * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
         * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} reduceContext.key - Key to be processed during the reduce stage
         * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
         *     for processing
         * @since 2015.2
         */
        const reduce = (reduceContext) => {
            try{
                let pdfArray = [];
                let htmlBody = '';
                setHtmlBody(htmlBody)
                reduceContext.values.forEach(value => {
                    let data = JSON.parse(value);
                    pdfCreation(pdfArray,data)
                    tableCreation(data,htmlBody)
                });
                htmlBody += '</table>';
                 
                
                log.debug({
                    title:"html",
                    details:htmlBody
                });
                sendEmail(pdfArray,reduceContext,htmlBody)

            }
            catch(e){
                log.debug({
                    title:e.name,
                    details:e.message
                });
            }

            
            

        }

        // create the pdf ,add it to pdfArray for further use
        function pdfCreation(pdfArray,data){
            

                let pdfFile = render.transaction({
                    entityId: Number(data.internalId),
                    printMode: render.PrintMode.PDF,
                    inCustLocale: true
                    });
                pdfArray.push(pdfFile)
            
        }

        // add the data into the table for further use
        function tableCreation(data,htmlBody){
            htmlBody += '<tr>';
            htmlBody += ' <td>' + data.invId + '</td>';
            htmlBody += ' <td>' + data.invDate + '</td>';
            htmlBody += ' <td>' + data.dueDate + '</td>';
            htmlBody += ' <td>' + data.amountRemaining + '</td>';
            htmlBody += ' <td>' + data.daysOverdue + '</td>';
            htmlBody += ' <td>' + data.totalAmount + '</td>';
            htmlBody += '</tr>';
            
        }

        // send email to the customer ,a single email is send to a customer which includes a collection of
        // its pdf as attachments and a table of invoice data as the body

        function sendEmail(pdfArray,reduceContext,htmlBody){
            email.send({
                author: -5,
                recipients: reduceContext.key,
                subject: 'You have invoices more than 30 days past due.Please check the attachments',
                body: htmlBody,
                attachments: pdfArray
            });
        }

        // setting the html format for the html table
        function setHtmlBody(htmlBody){
            
            htmlBody += '<table border = "1" >';
            // Header
            htmlBody += '<tr>';
            htmlBody += ' <td> Invoice Number </td>';
            htmlBody += ' <td> Invoice Date </td>';
            htmlBody += ' <td> Due Date</td>';
            htmlBody += ' <td> Amount Remaining </td>';
            htmlBody += ' <td> Days Overdue </td>';
            htmlBody += ' <td> Total Amount </td>';
            htmlBody += '</tr>';
        }



        /**
         * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
         * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
         * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
         * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
         *     script
         * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
         * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
         * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
         * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
         *     script
         * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
         * @param {Object} summaryContext.inputSummary - Statistics about the input stage
         * @param {Object} summaryContext.mapSummary - Statistics about the map stage
         * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
         * @since 2015.2
         */
        const summarize = (summaryContext) => {
            log.debug({
                title:"summary",
                details:summaryContext.reduceSummary
            });
        }

        return {getInputData, map, reduce, summarize}

    });