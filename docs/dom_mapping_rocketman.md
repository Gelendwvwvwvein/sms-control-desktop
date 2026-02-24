# DOM mapping: rocketman.ru (collector-debt)

Date: 2026-02-08

Notes:
- Do not store login URLs with tokens in docs/config. Token is passed at runtime.
- Prefer CSS selectors. XPath is fallback only.

## Page: Login
- page_name: Login
- url_pattern: https://rocketman.ru/manager/auth/login
- opened_from: direct link with token (runtime)
- requires_auth: no
- ready_condition: `#managerloginform-phone`

Auth fields:
- login_input_selector:
  - primary: `#managerloginform-phone`
  - fallback: `/html/body/div[2]/div/div/div/form/div[1]/input`
- password_input_selector:
  - primary: `#managerloginform-password`
  - fallback: `/html/body/div[2]/div/div/div/form/div[2]/input`
- submit_selector:
  - primary: `/html/body/div[2]/div/div/div/form/div[3]/button`
  - fallback: `/html/body/div[2]/div/div/div/form/div[3]/button`
- post_login_ready_selector:
  - primary: `#w1 > div.clearfix.filter-group-btn > div.pull-right > select`
  - fallback: `/html/body/div[1]/div/div[2]/form/div[4]/div[2]/select`
- login_error_selector:
  - not_used_in_flow: explicit login error selector is not configured; login result is validated by post-login URL/ready selector.

## Page: Clients list
- page_name: Clients list
- url_pattern: https://rocketman.ru/manager/collector-debt/work
- opened_from: login success
- requires_auth: yes
- ready_condition: `#w1 > div.clearfix.filter-group-btn > div.pull-right > select`

Actions:
- action_name: Select maximum available page size
  - when_used: after page load
  - primary_selector: `#w1 > div.clearfix.filter-group-btn > div.pull-right > select`
  - fallback_selector: `/html/body/div[1]/div/div[2]/form/div[4]/div[2]/select`
  - selector_type: css
  - expected_count: 1
  - after_action_expected: selected max numeric `value` from options
- action_name: Apply filters / show table
  - when_used: after selecting page size
  - primary_selector: `#w1 > div.clearfix.filter-group-btn > div.pull-left > button.btn.btn-primary`
  - fallback_selector: `/html/body/div[1]/div/div[2]/form/div[4]/div[1]/button[1]`
  - selector_type: css
  - expected_count: 1
  - after_action_expected: table rendered

Table:
- table_container_selector: `#w2-container > table`
- row_selector: `#w2-container > table > tbody > tr`
- row_unique_key_selector: `#w2-container > table > tbody > tr > td:nth-child(2) > a`
- summary_selector: `#w2 > div.summary`
- pagination_present: yes (required when rows > current page size)
- next_page_selector: `#w2 > ul > li.next > a`
- next_page_disabled_rule: class contains `disabled` on link or parent `li`
- page_size_selector: `#w1 > div.clearfix.filter-group-btn > div.pull-right > select`
- empty_state_selector: not used (no empty state in practice)

Client fields in table row:
- field: FIO
  - primary_selector: `td:nth-child(4) > a`
  - fallback_selector: `/html/body/div[1]/div/div[2]/div[2]/div/div[1]/table/tbody/tr[1]/td[4]/a`
  - selector_type: css
  - extract_mode: text
- field: Phone
  - primary_selector: `td:nth-child(5)`
  - fallback_selector: `/html/body/div[1]/div/div[2]/div[2]/div/div[1]/table/tbody/tr[1]/td[5]`
  - selector_type: css
  - extract_mode: text
- field: Timezone
  - primary_selector: `td:nth-child(12) > span`
  - fallback_selector: `/html/body/div[1]/div/div[2]/div[2]/div/div[1]/table/tbody/tr[1]/td[12]/span`
  - selector_type: css
  - extract_mode: text
- field: ContractStatusText
  - primary_selector: `td:nth-child(16)`
  - fallback_selector: `/html/body/div[1]/div/div[2]/div[2]/div/div[1]/table/tbody/tr[1]/td[16]`
  - selector_type: css
  - extract_mode: text
- field: DaysOverdue
  - column_header_selector: `#w2-container > table > thead > tr > th:nth-child(7)`
  - primary_selector: `td:nth-child(7)`
  - fallback_selector: `/html/body/div[1]/div/div[2]/div[2]/div/div[1]/table/tbody/tr[1]/td[7]`
  - selector_type: css/xpath
  - extract_mode: text
  - note: `daysOverdue` читается только из этой колонки, без эвристик и без подхвата из других полей
- field: ContractBlueFlag
  - primary_selector: `td:nth-child(2)`
  - fallback_selector: `/html/body/div[1]/div/div[2]/div[2]/div/div[1]/table/tbody/tr[1]/td[2]`
  - selector_type: css
  - extract_mode: class
  - rule: class contains `bg-info w2`

Client card link:
- link_selector: `td:nth-child(4) > a`
- link_attr: href
- url_pattern: https://rocketman.ru/manager/collector-comment/view?id=*

## Page: Client card
- page_name: Client card
- url_pattern: https://rocketman.ru/manager/collector-comment/view?id=*
- opened_from: click on FIO link in clients table
- requires_auth: yes
- ready_condition: `#w0`

Client card field:
- field: TotalWithCommission
  - primary_selector: `#w0 > tbody > tr:nth-child(6) > td`
  - fallback_selector: `/html/body/div[1]/div/div[2]/div[3]/div[2]/div[1]/div[1]/table/tbody/tr[6]/td`
  - selector_type: css
  - extract_mode: text

## Open items
- (none for daysOverdue selector)
