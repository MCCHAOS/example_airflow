TRUNCATE TABLE dwh."Fact_Feedback";

INSERT INTO dwh."Fact_Feedback" (
    "FeedbackID",
    "OrderID",
    "FeedbackScore",
    "FeedbackFormSentDate",
    "FeedbackAnswerDate"
)
SELECT
    feedback_id,
    order_id,
    feedback_score,
    TO_TIMESTAMP(feedback_form_sent_date, 'YYYY-MM-DD HH24:MI:SS'),
    TO_TIMESTAMP(feedback_answer_date, 'YYYY-MM-DD HH24:MI:SS')
FROM
    stage.feedback
WHERE
    feedback_form_sent_date IS NOT NULL
    AND feedback_form_sent_date ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$'
    AND feedback_answer_date IS NOT NULL
    AND feedback_answer_date ~ '^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$';