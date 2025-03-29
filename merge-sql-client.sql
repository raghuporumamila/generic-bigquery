-- Example Call:
CALL `your_project.your_dataset.usp_generic_merge`(
  'your_project.your_dataset.target_customers',  -- Target table
  'your_project.your_dataset.staging_customers', -- Source table
  ['customer_id'],                               -- Key column(s)
  NULL                                           -- Options (currently unused)
);

-- Example Call with composite key:
CALL `your_project.your_dataset.usp_generic_merge`(
  'your_project.your_dataset.target_orders',
  'your_project.your_dataset.staging_orders',
  ['order_id', 'order_line_item'],               -- Composite Key
  NULL
);
